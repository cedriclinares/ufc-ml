"""Chronological validation/refit/calibration and safe, self-contained checkpoints."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import platform

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from scipy.special import expit, logsumexp
import sklearn
import torch

from models.comparisons.evaluation import split_index
from .data import HISTORY_FIELDS
from .network import (
    FightTransformer,
    ModelConfig,
    Preprocessor,
    multitask_loss,
    probabilities,
    targets,
)


@dataclass
class TrainingConfig:
    epochs: int = 100
    batch_size: int = 64
    patience: int = 10
    learning_rate: float = 3e-4
    weight_decay: float = 0.01
    seed: int = 0
    device: str = "auto"
    threads: int = 4


def device_for(requested):
    if requested == "auto":
        return "mps" if torch.backends.mps.is_available() else "cpu"
    if requested == "mps" and not torch.backends.mps.is_available():
        raise ValueError("MPS requested but unavailable; use --device cpu")
    if requested.startswith("cuda") and not torch.cuda.is_available():
        raise ValueError("CUDA requested but unavailable")
    return requested


def chronological_parts(data):
    dates = data.rows.date
    a, b = split_index(dates, 0.70), split_index(dates, 0.85)
    if (
        not 0 < a < b < len(data)
        or dates.iloc[a - 1] >= dates.iloc[a]
        or dates.iloc[b - 1] >= dates.iloc[b]
    ):
        raise ValueError(
            "Need distinct event dates for fit, validation, and calibration"
        )
    return np.arange(a), np.arange(a, b), np.arange(b, len(data))


def model_for(config, preprocess, data):
    state = preprocess.state
    return FightTransformer(
        config,
        len(data.summary_fields),
        len(HISTORY_FIELDS),
        6 + len(state["gender"]) + 1 + (2 if config.odds_mode == "market" else 0),
        len(state["stance"]) + 1,
        len(state["method"]) + 1,
    )


def slice_batch(tensors, indices, device):
    return {k: v[indices].to(device) for k, v in tensors.items()}


def epoch(model, inputs, labels, config, device, optimizer=None):
    model.train(optimizer is not None)
    n = len(inputs["summary"])
    order = torch.randperm(n) if optimizer else torch.arange(n)
    total = 0.0
    with torch.set_grad_enabled(optimizer is not None):
        for start in range(0, n, config.batch_size):
            indices = order[start : start + config.batch_size]
            batch = slice_batch(inputs, indices, device)
            target = slice_batch(labels, indices, device)
            loss = multitask_loss(model(batch), target, batch["scheduled"])
            if not torch.isfinite(loss):
                raise ValueError("Non-finite training/validation loss")
            if optimizer:
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                optimizer.step()
            total += float(loss.detach()) * len(indices)
    return total / n


@torch.no_grad()
def predict_logits(model, preprocessor, data, device="cpu", batch_size=256):
    model.eval()
    inputs = preprocessor.transform(data, model.config.odds_mode)
    collected = {k: [] for k in ("winner", "rounds", "distance")}
    for start in range(0, len(data), batch_size):
        idx = torch.arange(start, min(start + batch_size, len(data)))
        result = model(slice_batch(inputs, idx, device))
        for k, v in result.items():
            collected[k].append(v.cpu())
    return {k: torch.cat(v) for k, v in collected.items()}


def calibrate(logits, data):
    temperatures, notes = {}, {}
    for task, column in [
        ("winner", "winner_target"),
        ("rounds", "rounds_target"),
        ("distance", "distance_target"),
    ]:
        actual = data.rows[column].to_numpy(float)
        mask = np.isfinite(actual)
        if mask.sum() < 2 or len(np.unique(actual[mask])) < 2:
            temperatures[task] = 1.0
            notes[task] = "Identity temperature: insufficient calibration outcomes"
            continue
        z = logits[task].numpy()[mask].astype(float)
        y = actual[mask]
        if task == "rounds":
            y = (y * 2).astype(int)

            def loss(log_t):
                scaled = z / np.exp(log_t)
                return float(
                    np.mean(logsumexp(scaled, axis=1) - scaled[np.arange(len(y)), y])
                )

        else:

            def loss(log_t):
                scaled = z / np.exp(log_t)
                return float(np.mean(np.logaddexp(0, scaled) - y * scaled))

        optimum = minimize_scalar(
            loss, bounds=(np.log(0.05), np.log(20)), method="bounded"
        )
        temperatures[task] = float(np.exp(optimum.x)) if optimum.success else 1.0
        notes[task] = dict(
            rows=int(mask.sum()),
            uncalibrated_nll=loss(0),
            calibrated_nll=loss(np.log(temperatures[task])),
        )
    return temperatures, notes


def fit_model(data, model_config, training_config):
    if len(data) < 10:
        raise ValueError("At least ten training fights are required")
    if (
        not data.rows[["winner_target", "rounds_target", "distance_target"]]
        .notna()
        .any()
        .any()
    ):
        raise ValueError("No valid training targets")
    device = device_for(training_config.device)
    torch.set_num_threads(training_config.threads)
    torch.manual_seed(training_config.seed)
    a, b, c = chronological_parts(data)
    fit, validation, calibration = (data.take(x) for x in (a, b, c))
    prep = Preprocessor().fit(fit)
    model = model_for(model_config, prep, data).to(device)
    inputs, labels = prep.transform(fit, model_config.odds_mode), targets(fit)
    vx, vy = prep.transform(validation, model_config.odds_mode), targets(validation)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=training_config.learning_rate,
        weight_decay=training_config.weight_decay,
    )
    best_loss, best_epoch, stale = float("inf"), 0, 0
    learning_curve = []
    for i in range(1, training_config.epochs + 1):
        train_loss = epoch(model, inputs, labels, training_config, device, optimizer)
        validation_loss = epoch(model, vx, vy, training_config, device)
        learning_curve.append(
            dict(epoch=i, train_loss=train_loss, validation_loss=validation_loss)
        )
        if validation_loss < best_loss - 1e-4:
            best_loss, best_epoch, stale = validation_loss, i, 0
        else:
            stale += 1
        if i == 1 or i % 5 == 0:
            print(
                f"    epoch {i}: train {train_loss:.4f}, validation {validation_loss:.4f}",
                flush=True,
            )
        if stale >= training_config.patience:
            break
    # Reinitialize and refit using validation-selected epoch count. Calibration remains untouched.
    refit = data.take(np.concatenate([a, b]))
    torch.manual_seed(training_config.seed)
    prep = Preprocessor().fit(refit)
    model = model_for(model_config, prep, data).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=training_config.learning_rate,
        weight_decay=training_config.weight_decay,
    )
    inputs, labels = prep.transform(refit, model_config.odds_mode), targets(refit)
    for _ in range(best_epoch):
        epoch(model, inputs, labels, training_config, device, optimizer)
    temperatures, notes = calibrate(
        predict_logits(model, prep, calibration, device), calibration
    )
    metadata = dict(
        created_utc=datetime.now(timezone.utc).isoformat(),
        training_config=asdict(training_config),
        model_config=asdict(model_config),
        selected_epoch=best_epoch,
        learning_curve=learning_curve,
        train_through=str(data.rows.date.max().date()),
        optimizer_train_through=str(refit.rows.date.max().date()),
        optimizer_train_ids=refit.rows.id.astype(int).tolist(),
        validation_ids=validation.rows.id.astype(int).tolist(),
        calibration_ids=calibration.rows.id.astype(int).tolist(),
        calibration_dates=[
            str(calibration.rows.date.min().date()),
            str(calibration.rows.date.max().date()),
        ],
        calibration=notes,
        temperatures=temperatures,
        classes=[i / 2 for i in range(11)],
        audit=data.audit,
        versions=dict(
            python=platform.python_version(),
            torch=str(torch.__version__),
            numpy=np.__version__,
            pandas=pd.__version__,
            sklearn=sklearn.__version__,
        ),
        odds_note="Historical moneyline capture times are unverified; total-market odds are never model inputs.",
    )
    return model, prep, temperatures, metadata


def predict(model, prep, temperatures, data, device="cpu"):
    logits = predict_logits(model, prep, data, device)
    return {k: v.numpy() for k, v in probabilities(logits, temperatures).items()}


def save_checkpoint(path, model, prep, temperatures, metadata):
    path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(
        dict(
            format_version=1,
            constructor=model.constructor(),
            state_dict={k: v.detach().cpu() for k, v in model.state_dict().items()},
            preprocessor=prep.state,
            temperatures=temperatures,
            metadata=metadata,
        ),
        path,
    )


def load_checkpoint(path, device="cpu"):
    checkpoint = torch.load(path, map_location="cpu", weights_only=True)
    if checkpoint.get("format_version") != 1:
        raise ValueError("Unsupported transformer checkpoint format")
    kwargs = dict(checkpoint["constructor"])
    kwargs["config"] = ModelConfig(**kwargs["config"])
    model = FightTransformer(**kwargs)
    model.load_state_dict(checkpoint["state_dict"])
    model.to(device).eval()
    return (
        model,
        Preprocessor(checkpoint["preprocessor"]),
        checkpoint["temperatures"],
        checkpoint["metadata"],
    )

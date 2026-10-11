"""Small shared encoders, swap-symmetric heads, and training-only preprocessing."""

from dataclasses import dataclass, asdict

import numpy as np
import torch
from torch import nn
from torch.nn import functional as F


def scaler(values):
    flat = np.asarray(values).reshape(-1, values.shape[-1])
    medians, means, scales = [], [], []
    for column in flat.T:
        known = column[np.isfinite(column)]
        median = float(np.median(known)) if len(known) else 0.0
        filled = np.where(np.isfinite(column), column, median)
        medians.append(median)
        means.append(float(filled.mean()) if len(filled) else 0.0)
        scales.append(
            float(filled.std()) if len(filled) and filled.std() > 1e-8 else 1.0
        )
    return dict(median=medians, mean=means, scale=scales)


def normalize(values, state):
    missing = ~np.isfinite(values)
    filled = np.where(missing, state["median"], values)
    return ((filled - state["mean"]) / state["scale"]).astype(
        np.float32
    ), missing.astype(np.float32)


def vocabulary(values):
    # Index zero is always unknown, including missing or previously unseen values.
    return {
        v: i + 1
        for i, v in enumerate(
            sorted(set(map(str, np.asarray(values).ravel())) - {"Unknown"})
        )
    }


def encode(values, vocab):
    return np.array(
        [vocab.get(str(v), 0) for v in values.ravel()], dtype=np.int64
    ).reshape(values.shape)


class Preprocessor:
    def __init__(self, state=None):
        self.state = state

    def fit(self, data):
        self.state = dict(
            summary=scaler(data.summary),
            history=scaler(data.history[~data.padding]),
            context=scaler(data.context),
            stance=vocabulary(data.stance),
            method=vocabulary(data.method[~data.padding]),
            gender=vocabulary(data.gender),
            summary_fields=data.summary_fields,
        )
        return self

    def transform(self, data, odds_mode="stats"):
        if data.summary_fields != self.state["summary_fields"]:
            raise ValueError("Checkpoint and input feature schemas differ")
        summary, smissing = normalize(data.summary, self.state["summary"])
        history, hmissing = normalize(data.history, self.state["history"])
        context, cmissing = normalize(data.context, self.state["context"])
        gender = np.eye(len(self.state["gender"]) + 1, dtype=np.float32)[
            encode(data.gender, self.state["gender"])
        ]
        context = np.concatenate([context, cmissing, gender], axis=-1)
        if odds_mode == "market":
            if not np.isfinite(data.market).all():
                raise ValueError(
                    "Odds-assisted models require valid prices for both fighters"
                )
            context = np.concatenate([context, data.market], axis=-1)
        arrays = dict(
            summary=summary,
            summary_missing=smissing,
            history=history,
            history_missing=hmissing,
            stance=encode(data.stance, self.state["stance"]),
            method=encode(data.method, self.state["method"]),
            padding=data.padding,
            context=context,
            scheduled=data.rows.number_of_rounds.to_numpy(dtype=np.int64),
        )
        return {
            k: torch.from_numpy(np.array(v, copy=True, order="C"))
            for k, v in arrays.items()
        }


@dataclass
class ModelConfig:
    architecture: str = "history"
    odds_mode: str = "stats"
    width: int = 64
    heads: int = 4
    layers: int = 2
    feedforward: int = 256
    dropout: float = 0.2
    history_length: int = 10


class FightTransformer(nn.Module):
    def __init__(
        self,
        config,
        summary_features,
        history_features,
        context_features,
        stance_categories,
        method_categories,
    ):
        super().__init__()
        self.config = config
        d = config.width
        self.stance_embedding = nn.Embedding(stance_categories, d)
        self.summary_token = nn.Parameter(torch.randn(1, 1, d) * 0.02)
        layer = nn.TransformerEncoderLayer(
            d,
            config.heads,
            config.feedforward,
            config.dropout,
            activation="relu",
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(
            layer, config.layers, norm=nn.LayerNorm(d), enable_nested_tensor=False
        )
        if config.architecture == "features":
            self.feature_weight = nn.Parameter(torch.randn(summary_features, d) * 0.02)
            self.feature_identity = nn.Parameter(
                torch.randn(summary_features, d) * 0.02
            )
            self.feature_missing = nn.Parameter(torch.randn(summary_features, d) * 0.02)
        elif config.architecture == "history":
            self.method_embedding = nn.Embedding(method_categories, d)
            self.history_projection = nn.Linear(history_features * 2 + d, d)
            self.position_embedding = nn.Embedding(config.history_length, d)
            self.summary_projection = nn.Linear(summary_features * 2 + d, d)
        else:
            raise ValueError("Architecture must be features or history")
        self.matchup = nn.Sequential(
            nn.Linear(d * 4 + context_features, 128),
            nn.ReLU(),
            nn.Dropout(config.dropout),
        )
        self.winner = nn.Linear(128, 1)
        self.rounds = nn.Linear(128, 11)
        self.distance = nn.Linear(128, 1)
        self.register_buffer("classes", torch.arange(11) / 2)

    def encode_fighters(self, batch):
        n = batch["summary"].shape[0]
        x = batch["summary"].flatten(0, 1)
        missing = batch["summary_missing"].flatten(0, 1)
        stance = self.stance_embedding(batch["stance"].flatten())
        cls = self.summary_token.expand(n * 2, -1, -1)
        if self.config.architecture == "features":
            tokens = (
                x.unsqueeze(-1) * self.feature_weight
                + self.feature_identity
                + missing.unsqueeze(-1) * self.feature_missing
            )
            tokens = torch.cat([cls, tokens, stance.unsqueeze(1)], dim=1)
            encoded = self.encoder(tokens)[:, 0]
        else:
            raw = torch.cat(
                [
                    batch["history"].flatten(0, 1),
                    batch["history_missing"].flatten(0, 1),
                    self.method_embedding(batch["method"].flatten(0, 1)),
                ],
                dim=-1,
            )
            tokens = self.history_projection(raw)
            positions = torch.arange(tokens.shape[1], device=tokens.device)
            tokens = tokens + self.position_embedding(positions)
            tokens = torch.cat([cls, tokens], dim=1)
            padding = torch.cat(
                [
                    torch.zeros(n * 2, 1, dtype=torch.bool, device=x.device),
                    batch["padding"].flatten(0, 1),
                ],
                dim=1,
            )
            encoded = self.encoder(tokens, src_key_padding_mask=padding)[:, 0]
            # Age, stature and lifetime summaries remain available alongside the sequence.
            encoded = encoded + self.summary_projection(
                torch.cat([x, missing, stance], dim=-1)
            )
        return encoded.reshape(n, 2, -1)

    def forward(self, batch):
        red, blue = self.encode_fighters(batch).unbind(dim=1)
        context = batch["context"]
        reverse_context = context
        if self.config.odds_mode == "market":
            reverse_context = torch.cat(
                [context[:, :-2], context[:, -2:].flip(-1)], dim=-1
            )
        forward = torch.cat([red, blue, red - blue, red * blue, context], dim=-1)
        reverse = torch.cat(
            [blue, red, blue - red, blue * red, reverse_context], dim=-1
        )
        a, b = self.matchup(torch.cat([forward, reverse])).chunk(2)
        winner = (self.winner(a) - self.winner(b)).squeeze(-1) / 2
        rounds = (self.rounds(a) + self.rounds(b)) / 2
        rounds = rounds.masked_fill(
            self.classes[None, :] > batch["scheduled"][:, None], float("-inf")
        )
        distance = (self.distance(a) + self.distance(b)).squeeze(-1) / 2
        return dict(winner=winner, rounds=rounds, distance=distance)

    def constructor(self):
        return dict(
            config=asdict(self.config),
            summary_features=(
                self.feature_weight.shape[0]
                if self.config.architecture == "features"
                else (self.summary_projection.in_features - self.config.width) // 2
            ),
            history_features=(
                (self.history_projection.in_features - self.config.width) // 2
                if self.config.architecture == "history"
                else 0
            ),
            context_features=self.matchup[0].in_features - self.config.width * 4,
            stance_categories=self.stance_embedding.num_embeddings,
            method_categories=(
                self.method_embedding.num_embeddings
                if self.config.architecture == "history"
                else 1
            ),
        )


def targets(data):
    return {
        name: torch.from_numpy(data.rows[column].to_numpy(dtype=np.float32))
        for name, column in [
            ("winner", "winner_target"),
            ("rounds", "rounds_target"),
            ("distance", "distance_target"),
        ]
    }


def multitask_loss(logits, labels, scheduled):
    loss = logits["winner"].sum() * 0
    for task in ("winner", "rounds", "distance"):
        mask = torch.isfinite(labels[task])
        if not mask.any():
            continue
        if task == "rounds":
            part = F.cross_entropy(
                logits[task][mask], (labels[task][mask] * 2).long(), reduction="none"
            )
            part = (part / torch.log(scheduled[mask].float() * 2 + 1)).mean()
        else:
            part = F.binary_cross_entropy_with_logits(
                logits[task][mask], labels[task][mask]
            )
        loss = loss + part * (0.5 if task == "distance" else 1.0)
    return loss


def probabilities(logits, temperatures=None):
    t = temperatures or dict(winner=1.0, rounds=1.0, distance=1.0)
    return dict(
        winner=torch.sigmoid(logits["winner"] / t["winner"]),
        rounds=torch.softmax(logits["rounds"] / t["rounds"], dim=-1),
        distance=torch.sigmoid(logits["distance"] / t["distance"]),
    )

"""Validate ancestry-level LD pair coverage statistics."""

from __future__ import annotations

import json
from pathlib import Path


def emit_empty_ld_pair_status(run_id: str, fine_mapping_locus_set_id: str, path: Path) -> str | None:
    """Return one locus-level status when any ancestry has zero LD pairs."""
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            n_ld_pairs = int(record["n_ld_pairs"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"Invalid LD pair statistics at line {line_number}") from error
        if n_ld_pairs == 0:
            return json.dumps(
                {
                    "runId": run_id,
                    "fineMappingLocusSetId": fine_mapping_locus_set_id,
                    "path": str(path),
                    "validationStage": "LD_ANNOTATION",
                    "reason": "EMPTY_LD_PAIRS",
                }
            )
    return None

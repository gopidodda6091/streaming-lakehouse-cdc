import json
from collections import Counter

from generate_events import generate, write_jsonl


def test_generator_is_deterministic_and_has_defects(tmp_path):
    records = generate(5_000, seed=7)
    assert records == generate(5_000, seed=7)

    topics = Counter(topic for topic, _, _ in records)
    assert topics["telemetry"] > topics["workorder_cdc"] > 0

    telemetry_values = [v for t, _, v in records if t == "telemetry"]
    assert len(telemetry_values) > len(set(telemetry_values)), "expected duplicate events"

    broken = 0
    for value in telemetry_values:
        try:
            json.loads(value)
        except json.JSONDecodeError:
            broken += 1
    assert broken > 0, "expected malformed JSON"

    write_jsonl(records, tmp_path)
    assert (tmp_path / "telemetry.jsonl").exists()

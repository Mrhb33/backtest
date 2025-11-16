import argparse
import csv
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable, List, Optional


def _fmt_percent(numerator: Decimal, denominator: Decimal) -> str:
    if denominator == 0:
        return ""
    percent = (numerator / denominator) * Decimal("100")
    quantized = percent.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def format_percentage_from_open(open_price: str, close_price: str) -> str:
    try:
        open_value = Decimal(open_price)
        close_value = Decimal(close_price)
        return _fmt_percent(close_value - open_value, open_value)
    except (InvalidOperation, ZeroDivisionError):
        return ""


def format_percentage_from_prev_close(prev_close: str, close_price: str) -> str:
    try:
        prev_close_value = Decimal(prev_close)
        close_value = Decimal(close_price)
        return _fmt_percent(close_value - prev_close_value, prev_close_value)
    except (InvalidOperation, ZeroDivisionError):
        return ""


def format_lower_wick(open_price: str, low_price: str) -> str:
    try:
        open_value = Decimal(open_price)
        low_value = Decimal(low_price)
        if open_value <= 0:
            return ""
        if low_value >= open_value:
            return "0.00000000"
        wick = (open_value - low_value) / open_value * Decimal("100")
        quantized = wick.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        return format(quantized, "f")
    except (InvalidOperation, ZeroDivisionError):
        return ""


def compute_time_utc(timestamp_ms: str) -> str:
    try:
        milliseconds = int(timestamp_ms)
        seconds = milliseconds / 1000
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return ""


def build_fieldnames(original: Iterable[str], add_time_utc: bool, replace_timestamp: bool) -> List[str]:
    original_list = list(original)
    filtered = [name for name in original_list if name not in {"percentage", "time_utc", "lower_wick_pct"}]
    if replace_timestamp and "timestamp" in filtered:
        filtered.remove("timestamp")
    extra = ["percentage", "lower_wick_pct"]
    if add_time_utc:
        extra.append("time_utc")
    return filtered + extra


def transform_csv(
    input_path: str,
    output_path: str | None = None,
    add_time_utc: bool = False,
    replace_timestamp: bool = False,
    start_time_utc: Optional[str] = None,
    step_seconds: int = 60,
) -> None:
    temp_path = output_path or f"{input_path}.tmp"
    final_path = output_path or input_path

    with open(input_path, newline="", encoding="utf-8") as infile, open(
        temp_path, "w", newline="", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        if reader.fieldnames is None:
            raise ValueError("Input CSV must have a header row.")

        fieldnames = build_fieldnames(reader.fieldnames, add_time_utc, replace_timestamp)
        timestamp_source_present = "timestamp" in (reader.fieldnames or [])
        time_utc_present = "time_utc" in (reader.fieldnames or [])
        base_time = datetime.fromisoformat(start_time_utc) if start_time_utc else None

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        prev_close_seen: Optional[str] = None

        for index, row in enumerate(reader):
            output_row: dict[str, str] = {}
            # Determine percentage precedence:
            # 1) If input already has pct_change_prev_close, mirror it into "percentage"
            # 2) Else compute from previous row close if available
            # 3) Else fallback to open->close percentage
            pct_from_input = row.get("pct_change_prev_close", "")
            if pct_from_input and pct_from_input.strip() != "":
                percentage = pct_from_input.strip()
            elif prev_close_seen is not None:
                percentage = format_percentage_from_prev_close(prev_close_seen, row.get("close", ""))
            else:
                percentage = format_percentage_from_open(row.get("open", ""), row.get("close", ""))

            lower_wick_pct = format_lower_wick(row.get("open", ""), row.get("low", ""))
            time_utc_value = row.get("time_utc", "") if time_utc_present else ""
            if add_time_utc:
                if timestamp_source_present:
                    time_utc_value = compute_time_utc(row.get("timestamp", ""))
                elif base_time is not None:
                    time_utc_value = (base_time + timedelta(seconds=step_seconds * index)).isoformat()

            for key in fieldnames:
                if key == "percentage":
                    output_row[key] = percentage
                elif key == "lower_wick_pct":
                    output_row[key] = lower_wick_pct
                elif key == "time_utc":
                    output_row[key] = time_utc_value
                else:
                    output_row[key] = row.get(key, "")

            writer.writerow(output_row)

            # Track previous close for next row computation when needed
            close_curr = row.get("close", "")
            prev_close_seen = close_curr if close_curr is not None else prev_close_seen

    if output_path is None:
        os.replace(temp_path, final_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append percentage change column to OHLCV CSV data.")
    parser.add_argument("--input", required=True, help="Path to the input CSV file.")
    parser.add_argument(
        "--output",
        help="Optional path for the output CSV. If omitted, the input file is modified in-place.",
    )
    parser.add_argument(
        "--add-time-utc",
        action="store_true",
        help="Also add a RFC3339 time_utc column derived from the timestamp column.",
    )
    parser.add_argument(
        "--replace-timestamp",
        action="store_true",
        help="When adding time_utc, drop the original numeric timestamp column.",
    )
    parser.add_argument(
        "--start-time-utc",
        help=(
            "Optional ISO8601 timestamp to seed time_utc when the source CSV lacks a timestamp column. "
            "Used with --add-time-utc."
        ),
    )
    parser.add_argument(
        "--step-seconds",
        type=int,
        default=60,
        help="Interval in seconds between candles when using --start-time-utc. Default is 60.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    transform_csv(
        args.input,
        args.output,
        args.add_time_utc,
        args.replace_timestamp,
        args.start_time_utc,
        args.step_seconds,
    )


if __name__ == "__main__":
    main()
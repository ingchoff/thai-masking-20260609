"""
Step 3 of masking pipeline: Process timestamp data from input tables.
Single-file refactor – 100 % functional parity.
"""

from __future__ import annotations

import argparse
import unicodedata
import json
import json5
import os
import re
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import tiktoken
from dotenv import load_dotenv
from jamaibase import JamAI, protocol as p
from jamaibase.utils.exceptions import JamaiException, ResourceNotFoundError

# --------------------------------------------------------------------------- #
# 0. Bootstrap logging
# --------------------------------------------------------------------------- #
try:
    from Masking.logging_utils import LOGGER as _BASE_LOGGER
except ImportError:
    from logging_utils import LOGGER as _BASE_LOGGER

try:
    from Masking.jamai_utils import resolve_jamai_timeout
except ImportError:
    from jamai_utils import resolve_jamai_timeout


_LOG = _BASE_LOGGER.bind(type="pipeline", service_name="get_timestamps")

# --------------------------------------------------------------------------- #
# 1. Pure configuration object
# --------------------------------------------------------------------------- #
load_dotenv()

class _Config:
    """Immutable configuration built once from env + CLI overrides."""
    __slots__ = (
        "project_id",
        "api_base",
        "step2_prefix",
        "out_schema_id",
        "out_prefix",
        "context_limit",
        "fetch_limit",
        "default_fetch_limit",
        "timeout",
        "particles",
        "thai_digits",
        "card_terms",
        "keywords_to_track",
    )

    def __init__(self, cli: Optional[argparse.Namespace] = None) -> None:
        self.project_id: str = os.getenv("PROJECT_ID", "proj_d51957697af3bcec339092cb")
        self.api_base: str = os.getenv("JAMAI_API_BASE", "http://localhost:6969/api")
        self.step2_prefix = "step2b_"
        self.out_schema_id = "step3"
        self.out_prefix = "step3_"
        self.context_limit: int = int(os.getenv("MODEL_CONTEXT_LIMIT", "32000"))
        self.fetch_limit: int = 100
        self.default_fetch_limit = 100
        self.timeout: float = resolve_jamai_timeout(logger=_LOG)
        
        # Keyword lists for timestamp injection
        self.particles = ["ครับ", "ค่ะ", "คะ", "คับ", "นะ", "นะครับ", "นะคะ", "จ้ะ", "จ้า", "หน่อย", "สิ", "เถอะ", "แหละ", "หรอก", "ฮะ", "วะ", "ขอรับ", "เพคะ"]
        self.thai_digits = ["ศูนย์", "หนึ่ง", "สอง", "สาม", "สี่", "ห้า", "หก", "เจ็ด", "แปด", "เก้า", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        self.card_terms = [
            # Individual important terms
            "บัตร",
            "เครดิต", 
            "เดบิต",
            "รหัส",
            "หมดอายุ",
            "วีซ่า",
            "มาสเตอร์",
            # Compound terms
            "บัตรเครดิต", 
            "บัตรเดบิต", 
            "เลขบัตร", 
            "รหัสบัตร", 
            "หมดอายุบัตร", 
            "ทับ", 
            "เอ็กซ์ไพรี่", 
            "ซีวีวี",
            # English terms
            "call center",
            "center"
        ]
        # For this specific use case, we only care about the card terms
        self.keywords_to_track = self.card_terms
        
        if cli:
            # CLI overrides
            self.context_limit = cli.context_limit

CFG = _Config()  # default instance; CLI will mutate below

# --------------------------------------------------------------------------- #
# 2. Shared helpers
# --------------------------------------------------------------------------- #
_TOKENIZER = tiktoken.get_encoding("o200k_base")

def _count_tokens(text: str | None) -> int:
    if not text:
        return 0
    try:
        return len(_TOKENIZER.encode(text))
    except Exception:
        return len(text or "") // 4  # fallback

def _extract_value(field: Any) -> Any:
    return field.get("value") if isinstance(field, dict) else field

def _safe_json(text: str) -> Any:
    if not text:
        return None
    # 1. Try to extract <JSON>…</JSON> first
    m = re.search(r"<JSON>([\s\S]*?)</JSON>", text, re.I)
    payload = m.group(1) if m else text.strip()

    # 2. Parse with JSON5
    try:
        return json5.loads(payload)
    except Exception as e:
        _LOG.error(f"Failed to parse JSON: {m}, err: {str(e)}")
        return None


def _parse_ts(table_id: str, prefix: str) -> str | None:
    if not table_id.startswith(prefix):
        return None
    ts = table_id[len(prefix) :]
    try:
        datetime.strptime(ts, "%Y%m%d_%H%M%S")
        return ts
    except ValueError:
        return None


def generate_final_transcript_string(
    segments: list,
    keywords: list,
    digits: list = None,
    merge_gap_tolerance: float = 0.01
) -> str:
    """
    Generates a final transcript with each segment on a separate line with timestamps.
    
    This function handles ASR systems that output character-level tokens by:
    1. Merging adjacent segments from the same speaker to create coherent turns.
    2. Reconstructing full text from character-level tokens.
    3. Using regex to find keywords and digit sequences in the full text.
    4. Creating separate lines for each segment with precise timestamps.

    Args:
        segments (list): The raw list of segment dictionaries, each with a 'words' list.
        keywords (list): A list of keywords to highlight with timestamps.
        digits (list): A list of digit words to detect as sequences.
        merge_gap_tolerance (float): The maximum time gap (in seconds) between segments
                                     to be considered for merging.

    Returns:
        str: A multi-line string representing the final, formatted conversation.
            Format: [start --> end] [channel]: text
    """
    if not segments:
        return ""
    
    if digits is None:
        digits = CFG.thai_digits

    # --- Part 1: Merge Segments ---
    from collections import defaultdict
    import copy
    import re
    
    # Group segments by speaker
    speaker_segments = defaultdict(list)
    for seg in sorted(segments, key=lambda x: x.get("start", 0)):
        speaker_segments[seg.get("channel", "Unknown")].append(seg)

    merged_segments = []
    for speaker, segs_by_speaker in speaker_segments.items():
        if not segs_by_speaker:
            continue
        
        # Use deepcopy to create a fully independent copy
        current_turn = copy.deepcopy(segs_by_speaker[0])
        
        for next_seg in segs_by_speaker[1:]:
            gap = next_seg.get("start", 0) - current_turn.get("end", 0)
            if gap <= merge_gap_tolerance:
                # Merge
                current_turn["end"] = max(next_seg.get("end", 0), current_turn.get("end", 0))
                if "words" in next_seg:
                    current_turn.setdefault("words", []).extend(next_seg.get("words", []))
            else:
                # Finish current turn and start a new one
                merged_segments.append(current_turn)
                current_turn = copy.deepcopy(next_seg)
        
        merged_segments.append(current_turn)  # Add the last turn

    # Sort all final turns by start time
    merged_segments.sort(key=lambda x: x.get("start", 0))

    # --- Part 2: Character-Mapping and Segment Splitting ---
    output_lines = []
    
    # Prepare regex patterns
    keyword_pattern = "|".join(re.escape(kw) for kw in sorted(keywords, key=len, reverse=True))
    digit_pattern = "|".join(re.escape(d) for d in digits)
    digit_sequence_pattern = f"(?:(?:{digit_pattern})\\s*)+"
    
    # Combined pattern to find either a keyword or a digit sequence
    combined_pattern = re.compile(f"({keyword_pattern}|{digit_sequence_pattern})", re.IGNORECASE)

    for turn in merged_segments:
        channel = turn.get("channel", "Unknown")
        words = turn.get("words", [])
        if not words:
            continue

        # Reconstruct full text and create the character-to-time map
        full_text = ""
        char_map = {}  # Maps index in full_text to the original word/char info
        for char_info in words:
            char_token = char_info.get("word", "")
            if char_token:  # Ignore empty tokens
                char_map[len(full_text)] = char_info
                full_text += char_token
        
        # Find all matches in the reconstructed text
        matches = list(combined_pattern.finditer(full_text))
        
        # Create segments based on matches
        segments = []
        last_index = 0
        
        for match in matches:
            match_start_idx, match_end_idx = match.span()
            
            # Add text before match as a segment
            if last_index < match_start_idx:
                pre_text = full_text[last_index:match_start_idx].strip()
                if pre_text:
                    # Get timing for the pre-match text
                    first_char_info_idx = last_index
                    while first_char_info_idx not in char_map and first_char_info_idx < match_start_idx:
                        first_char_info_idx += 1
                    
                    last_char_info_idx = match_start_idx - 1
                    while last_char_info_idx not in char_map and last_char_info_idx > last_index:
                        last_char_info_idx -= 1
                    
                    if first_char_info_idx in char_map and last_char_info_idx in char_map:
                        start_time = char_map[first_char_info_idx].get('start', 0.0)
                        end_time = char_map[last_char_info_idx].get('end', 0.0)
                        segments.append({
                            'text': pre_text,
                            'start': start_time,
                            'end': end_time
                        })
            
            # Add the match as a separate segment
            matched_text = full_text[match_start_idx:match_end_idx].strip()
            if matched_text:
                # Get timing for the matched text
                first_char_info_idx = match_start_idx
                while first_char_info_idx not in char_map and first_char_info_idx < match_end_idx:
                    first_char_info_idx += 1
                
                last_char_info_idx = match_end_idx - 1
                while last_char_info_idx not in char_map and last_char_info_idx > match_start_idx:
                    last_char_info_idx -= 1
                
                if first_char_info_idx in char_map and last_char_info_idx in char_map:
                    start_time = char_map[first_char_info_idx].get('start', 0.0)
                    end_time = char_map[last_char_info_idx].get('end', 0.0)
                    segments.append({
                        'text': matched_text,
                        'start': start_time,
                        'end': end_time
                    })
            
            last_index = match_end_idx
        
        # Add any remaining text after the last match
        if last_index < len(full_text):
            remaining_text = full_text[last_index:].strip()
            if remaining_text:
                first_char_info_idx = last_index
                while first_char_info_idx not in char_map and first_char_info_idx < len(full_text):
                    first_char_info_idx += 1
                
                last_char_info_idx = len(full_text) - 1
                while last_char_info_idx not in char_map and last_char_info_idx > last_index:
                    last_char_info_idx -= 1
                
                if first_char_info_idx in char_map and last_char_info_idx in char_map:
                    start_time = char_map[first_char_info_idx].get('start', 0.0)
                    end_time = char_map[last_char_info_idx].get('end', 0.0)
                    segments.append({
                        'text': remaining_text,
                        'start': start_time,
                        'end': end_time
                    })
        
        # Format each segment as a separate line
        for segment in segments:
            timestamp_with_channel = f"[{segment['start']:.2f} --> {segment['end']:.2f}] [{channel}]:"
            output_lines.append(f"{timestamp_with_channel} {segment['text']}")

    return "\n".join(output_lines)


def generate_final_transcript_string_time_based(
    segments: List[Dict[str, Any]],
    split_threshold_sec: float = 20.0,
    target_line_len_sec: float = 12.0,
    tail_merge_threshold_sec: float = 3.0,  # if the remaining tail < this, absorb into current chunk
) -> str:
    """
    Non-destructive splitter:
      - DOES NOT MERGE across segments.
      - If segment duration <= split_threshold_sec: emit exactly as-is.
      - If longer: split around target_line_len_sec with Thai/number-safe boundaries.
      - Never split inside numeric runs or across Thai grapheme-sensitive joins.
      - Avoid tiny trailing chunks via tail_merge_threshold_sec.

    Expected segment:
      {
        "start": float, "end": float, "channel": str,
        "words": [{"word": str, "start": float, "end": float}, ...],
        # optional fallback:
        "text": str
      }

    Output format:
      [chunk_start --> chunk_end] [channel]: <concatenated token text>
    """

    # ---------------- Unicode & token helpers ----------------
    THAI_PREPOSED_VOWELS = set("\u0E40\u0E41\u0E42\u0E43\u0E44")  # เ แ โ ใ ไ
    NUM_CONNECTORS = {"-", "–", "—", "−", "/", ":", ".", ",", "(", ")", "+", "×"}
    PUNCT_CATS = {"Pc", "Pd", "Ps", "Pe", "Pi", "Pf", "Po"}

    THAI_NUM_BLOCKS = (
        "ศูนย์|นึง|หนึ่ง|สอง|สาม|สี่|ห้า|หก|เจ็ด|แปด|เก้า|สิบ|ยี่|เอ็ด|ร้อย|พัน|หมื่น|แสน|ล้าน|จุด|ลบ"
    )
    RE_THAI_WORD_NUMBER = re.compile(rf"^(?:{THAI_NUM_BLOCKS})+$")
    RE_NUMERIC_WITH_SEPS = re.compile(r"^[+\-−]?\d{1,4}(?:[ ,.\-–—:/]\d{1,4})*(?:\.\d+)?%?$")

    def _cat(ch: str) -> str:
        return unicodedata.category(ch) if ch else ""

    def _first_char(s: str) -> str:
        return s[0] if s else ""

    def _last_char(s: str) -> str:
        return s[-1] if s else ""

    def is_mark(ch: str) -> bool:
        # Combining marks (Thai tone/diacritics)
        return _cat(ch).startswith("M")

    def is_thai_letter(ch: str) -> bool:
        return "\u0E00" <= ch <= "\u0E7F" and _cat(ch) == "Lo"

    def is_punct_or_space_token(tok: str) -> bool:
        if not tok:
            return False
        for ch in tok:
            if ch.isspace():
                continue
            if _cat(ch) not in PUNCT_CATS:
                return False
        return True

    def is_numberish_token(tok: str) -> bool:
        """Arabic/Thai digits, digits+separators, or Thai number-word compounds."""
        if not tok:
            return False
        t = unicodedata.normalize("NFC", tok.replace("\u200b", "").strip())
        if not t:
            return False
        if t.isdecimal():
            return True
        if RE_NUMERIC_WITH_SEPS.fullmatch(t):
            return True
        if RE_THAI_WORD_NUMBER.fullmatch(t):
            return True
        return False

    def is_numeric_connector(tok: str) -> bool:
        t = unicodedata.normalize("NFC", (tok or "").strip())
        return t in NUM_CONNECTORS or t == "จุด"  # Thai word for decimal point

    def boundary_is_safe(left_tok: str, right_tok: str) -> bool:
        """
        SAFE when it's a natural break.
        UNSAFE if:
          - numeric run (numberish/connector on both sides),
          - cutting around combining marks,
          - cutting after preposed vowels (เ แ โ ใ ไ),
          - both sides are Thai letters (likely mid-word).
        Prefer SAFE if punctuation/space is at the seam.
        """
        # numeric run protection
        if (is_numberish_token(left_tok) or is_numeric_connector(left_tok)) and \
           (is_numberish_token(right_tok) or is_numeric_connector(right_tok)):
            return False

        if is_punct_or_space_token(left_tok) or is_punct_or_space_token(right_tok):
            return True

        L = _last_char(left_tok)
        R = _first_char(right_tok)

        if is_mark(L) or is_mark(R):
            return False
        if L in THAI_PREPOSED_VOWELS:
            return False
        if is_thai_letter(L) and is_thai_letter(R):
            return False
        return True

    def join_tokens_exact(tokens: List[Dict[str, Any]]) -> str:
        return "".join(str(t.get("word", "")) for t in tokens)

    # ---------------- Splitting inside a single segment ----------------
    def split_tokens(tokens: List[Dict[str, Any]], seg_start: float, seg_end: float) -> List[List[Dict[str, Any]]]:
        """
        Split near target_line_len_sec; nudge to nearest SAFE boundary; avoid tiny tails.
        Operates ONLY within this segment (no cross-segment effects).
        """
        if not tokens:
            return []
        toks = sorted(tokens, key=lambda t: t.get("start", seg_start))
        out, i, n = [], 0, len(toks)

        def seam_safe_idx(idx: int) -> bool:
            if not (i < idx <= n - 1):
                return False
            return boundary_is_safe(str(toks[idx - 1].get("word", "")), str(toks[idx].get("word", "")))

        while i < n:
            chunk_start = float(toks[i].get("start", seg_start))
            remaining_total = seg_end - chunk_start

            # Entire remainder is short → swallow it to avoid tiny last chunk
            if remaining_total <= tail_merge_threshold_sec:
                out.append(toks[i:n])
                break

            # Remainder close to target → just take the rest
            if remaining_total <= target_line_len_sec * 1.05:
                out.append(toks[i:n])
                break

            target_time = chunk_start + target_line_len_sec

            # Find first token whose end >= target_time
            j, cut_at = i, None
            while j < n:
                if float(toks[j].get("end", seg_end)) >= target_time:
                    cut_at = j + 1  # cut AFTER j
                    break
                j += 1
            if cut_at is None:
                out.append(toks[i:n])
                break

            # Search forward (<=2s or 40 tokens) for SAFE boundary
            k = cut_at
            max_forward_time = target_time + 2.0
            max_forward_idx = min(n - 1, cut_at + 40)
            while k <= max_forward_idx and float(toks[k - 1].get("end", seg_end)) <= max_forward_time:
                if seam_safe_idx(k):
                    cut_at = k
                    break
                k += 1

            # If still unsafe, search backward (<=2s or 40 tokens)
            if not seam_safe_idx(cut_at):
                k = cut_at
                min_back_time = target_time - 2.0
                min_back_idx = max(i + 1, cut_at - 40)
                while k >= min_back_idx and float(toks[k - 1].get("end", seg_start)) >= min_back_time:
                    if seam_safe_idx(k):
                        cut_at = k
                        break
                    k -= 1

            # Numeric run guard: if boundary is inside a numeric run, push to run end
            if cut_at < n:
                L = str(toks[cut_at - 1].get("word", ""))
                R = str(toks[cut_at].get("word", "")) if cut_at < n else ""
                if (is_numberish_token(L) or is_numeric_connector(L)) and \
                   (is_numberish_token(R) or is_numeric_connector(R)):
                    m = cut_at
                    while m < n:
                        nxt = str(toks[m].get("word", ""))
                        if not (is_numberish_token(nxt) or is_numeric_connector(nxt)):
                            break
                        m += 1
                    cut_at = m

            # Avoid tiny last tail: if tail after cut < threshold, take the rest now
            cut_end_time = float(toks[cut_at - 1].get("end", seg_end))
            if (seg_end - cut_end_time) <= tail_merge_threshold_sec:
                cut_at = n

            # Safety
            if cut_at <= i:
                cut_at = min(i + 1, n)

            out.append(toks[i:cut_at])
            i = cut_at

        return out

    # ---------------- Emit lines (no cross-segment merging) ----------------
    lines: List[str] = []
    for seg in sorted(segments or [], key=lambda s: s.get("start", 0.0)):
        seg_start = float(seg.get("start", 0.0))
        seg_end   = float(seg.get("end", seg_start))
        channel   = seg.get("channel", "Unknown")
        tokens    = [t for t in (seg.get("words") or []) if str(t.get("word", "")) != ""]
        duration  = max(0.0, seg_end - seg_start)

        # If segment is within threshold → emit EXACTLY this segment (no changes)
        if duration <= split_threshold_sec or not tokens:
            text = join_tokens_exact(tokens) if tokens else str(seg.get("text", "")).strip()
            lines.append(f"[{seg_start:.2f} --> {seg_end:.2f}] [{channel}]: {text}")
            continue

        # Otherwise, split this segment safely
        chunks = split_tokens(tokens, seg_start, seg_end)
        for chunk in chunks:
            if not chunk:
                continue
            c_start = float(chunk[0].get("start", seg_start))
            c_end   = float(chunk[-1].get("end", seg_end))
            text    = join_tokens_exact(chunk)
            lines.append(f"[{c_start:.2f} --> {c_end:.2f}] [{channel}]: {text}")

    return "\n".join(lines)

# --------------------------------------------------------------------------- #
# 3. JamAI table discovery
# --------------------------------------------------------------------------- #
class _TableResolver:
    """Handles all table-id resolution and creation."""
    def __init__(self, jamai: JamAI) -> None:
        self.jamai = jamai

    def discover(
        self,
        step2_override: Optional[str],
    ) -> Tuple[str, str]:
        # ---------- step2 ----------
        if step2_override:
            step2_id = step2_override
            ts = _parse_ts(step2_id, CFG.step2_prefix) or datetime.now().strftime("%Y%m%d_%H%M%S")
        else:
            step2_id, ts = self._latest(CFG.step2_prefix)
            if not step2_id:
                raise ResourceNotFoundError(f"No table with prefix '{CFG.step2_prefix}'")
        # ---------- output ----------
        out_id = f"{CFG.out_prefix}{ts}"
        self._setup_output(out_id)
        return step2_id, out_id

    # ---------------- internal ---------------- #
    def _latest(self, prefix: str) -> Tuple[str | None, str | None]:
        tables = self.jamai.table.list_tables(table_type=p.TableType.ACTION).items
        candidates = [
            (datetime.strptime(ts, "%Y%m%d_%H%M%S"), tbl.id, ts)
            for tbl in tables
            if (ts := _parse_ts(tbl.id, prefix))
        ]
        if not candidates:
            return None, None
        _, tbl_id, ts = max(candidates, key=lambda x: x[0])
        return tbl_id, ts

    def _ensure_exists(self, table_id: str) -> None:
        try:
            self.jamai.table.get_table(table_type=p.TableType.ACTION, table_id=table_id)
        except ResourceNotFoundError:
            raise ResourceNotFoundError(f"Step 1 table '{table_id}' not found")

    def _setup_output(self, out_id: str) -> None:
        self.jamai.table.get_table(table_type=p.TableType.ACTION, table_id=CFG.out_schema_id)
        try:
            self.jamai.table.delete_table(table_type=p.TableType.ACTION, table_id=out_id)
        except ResourceNotFoundError:
            pass
        self.jamai.table.duplicate_table(
            table_type=p.TableType.ACTION,
            table_id_src=CFG.out_schema_id,
            table_id_dst=out_id,
            include_data=False,
        )

# --------------------------------------------------------------------------- #
# 4. Business logic – pure functions
# --------------------------------------------------------------------------- #
class _Logic:
    """All data transformation without side effects."""

    @staticmethod
    def overlap_len(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
        """Compute overlap length between two time ranges."""
        return min(a_end, b_end) - max(a_start, b_start)

    @staticmethod
    def is_overlap(a_start: float, a_end: float, b_start: float, b_end: float, epsilon: float = 0.5) -> bool:
        """Check if two time ranges overlap, inclusive."""
        return _Logic.overlap_len(a_start, a_end, b_start, b_end) > -epsilon

    # ---------- segment filtering by time ----------
    @staticmethod
    def overlapping_segments(segments_raw: Any, start: float, end: float) -> List[Dict]:
        if isinstance(segments_raw, str):
            segments = _safe_json(segments_raw)
        elif isinstance(segments_raw, list):
            segments = segments_raw
        else:
            segments = None
        if not isinstance(segments, list):
            return []
        return [
            s
            for s in segments
            if isinstance(s, dict)
            and _Logic.is_overlap(s["start"], s["end"], start, end)
        ]


# --------------------------------------------------------------------------- #
# 5. Orchestration
# --------------------------------------------------------------------------- #
class _Processor:
    def __init__(self, jamai: JamAI) -> None:
        self.jamai = jamai
        self.resolver = _TableResolver(jamai)

    def run(
        self,
        *,
        step2: Optional[str],
        context_limit: int,
        row_limit: Optional[int],
    ) -> str:
        step2_id, out_id = self.resolver.discover(step2)
        input_table = self.jamai.table.get_table(table_type=p.TableType.ACTION, table_id=step2_id)
        rows_total = 0
        limit = row_limit if row_limit is not None else float("inf")

        total_rows = input_table.num_rows
        offset = 0
        _row_num = 0
        all_rows: list[dict[str, Any]] = []
        while _row_num < total_rows:
            step2_rows = self.jamai.table.list_table_rows(
                table_type=p.TableType.ACTION,
                table_id=step2_id,
                columns=[
                    "file_path",
                    "Segment_ID",
                    "Transcription",
                    "SegmentsAndWords",
                ],
                limit=CFG.default_fetch_limit,
                offset=offset,
            )
            all_rows.extend(step2_rows.items)
            offset += len(step2_rows.items)
            _row_num += len(step2_rows.items)
        grouped = {}
        for r in all_rows:
            key = _extract_value(r.get("file_path"))
            if key:
                grouped.setdefault(key, []).append(r)
        _LOG.info(f"Found {len(grouped)} unique file groups to process.")
        for idx, (row_key, chunk_rows) in enumerate(grouped.items(), 1):
            if rows_total >= limit:
                _LOG.info("Row limit reached")
                break
            try:
                self._process_group(row_key, chunk_rows, out_id, context_limit)
            except Exception as e:
                _LOG.error(f"Error processing group {row_key}: {e}")
                _LOG.error(traceback.format_exc())
            rows_total += 1
        return out_id

    # ---------- per group ----------
    def _process_group(
        self,
        row_key: str,
        rows: List,
        out_id: str,
        context_limit: int,
    ) -> None:
        _LOG.info(f"Processing group {row_key}")
        first = rows[0]
        file_path = _extract_value(first.get("file_path"))

        phases_raw = None
        payloads = []
        for r in rows:
            if seg_id := _extract_value(r.get("Segment_ID")):
                phase = _safe_json(seg_id)
            if not phase:
                continue
            
            if not _extract_value(phase.get("include_PAN")):
                _LOG.info(f"No PAN phases for phase id: {_extract_value(phase.get('index'))}")
                continue

            seg_data = _extract_value(r.get("SegmentsAndWords"))
            segs = _Logic.overlapping_segments(seg_data, phase["start_time"], phase["end_time"])
            if not segs:
                continue
            
            # Generate final transcript string with merged segments and keyword timestamps
            # final_cleaned = generate_final_transcript_string(
            #     segments=segs,
            #     keywords=CFG.keywords_to_track,
            #     digits=CFG.thai_digits
            # )
            final_cleaned = generate_final_transcript_string_time_based(
                segments=segs,
            )

            payload = {
                "file_path": file_path,
                "Explain": f"Phase {phase['index']}: {phase['description']}",
                "Segment_ID": f"Phase_{phase['index']}_{phase['phase']}",
                "Transcription": _extract_value(r.get("Transcription")),
                "SegmentsAndWords": final_cleaned,
            }
            payloads.append(payload)
        if len(payloads) > 0:
            self.jamai.table.add_table_rows(
                table_type=p.TableType.ACTION,
                request=p.RowAddRequest(table_id=out_id, data=payloads, stream=False),
            )

# --------------------------------------------------------------------------- #
# 6. Public API (unchanged signatures)
# --------------------------------------------------------------------------- #
def run_get_ts_step_pipeline(
    tasks: List[Dict[str, Any]],
    input_table_step2: Optional[str] = None,
    context_limit: int = CFG.context_limit,
    row_limit: Optional[int] = None,
    jamai_client: Optional[JamAI] = None,
) -> Dict[str, Any]:
    try:
        jamai = jamai_client or JamAI(
            project_id=CFG.project_id,
            api_base=CFG.api_base,
            timeout=CFG.timeout,
        )
        processor = _Processor(jamai)
        table_id = processor.run(
            step2=input_table_step2,
            context_limit=context_limit,
            row_limit=row_limit,
        )
        return {"tasks": tasks, "table_id": table_id, "stats": {"status": "success", "message": "Success"}}
    except Exception as e:
        return {"tasks": tasks, "table_id": None, "stats": {"status": "failed", "message": str(e)}}

def run_get_ts_step(
    input_table_step2: Optional[str] = None,
    context_limit: int = CFG.context_limit,
    row_limit: Optional[int] = None,
    jamai_client: Optional[JamAI] = None,
) -> str:
    jamai = jamai_client or JamAI(
        project_id=CFG.project_id,
        api_base=CFG.api_base,
        timeout=CFG.timeout,
    )
    return _Processor(jamai).run(
        step2=input_table_step2,
        context_limit=context_limit,
        row_limit=row_limit,
    )

# --------------------------------------------------------------------------- #
# 7. CLI (unchanged)
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch detailed segments based on phase analysis, process/reduce them, and upload to a timestamped output table."
    )
    parser.add_argument("--input-table-step2", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--context-limit", type=int, default=CFG.context_limit)
    args = parser.parse_args()

    # update global config with CLI values
    CFG = _Config(args)

    try:
        tid = run_get_ts_step(
            input_table_step2=args.input_table_step2,
            context_limit=args.context_limit,
            row_limit=args.limit,
        )
        _LOG.info(f"Finished. Output table: {tid}")
    except Exception as e:
        _LOG.error(str(e))
        sys.exit(1)
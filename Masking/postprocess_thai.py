# postprocess_thai.py
import re
from typing import List, Dict


GAP_TOL = 0.01  # seconds - increased to handle typical speech gaps
MANUAL_FIX = {"ชค": "เช็ก", "องคร": "องค์กร"}

class ThaiTranscriptCleaner:
    """
    A lightweight post-processor for Thai transcripts that merges fragmented segments,
    normalizes whitespace, and applies basic spell correction.
    """
    
    def merge_segments(self, segments: List[Dict]) -> List[Dict]:
        """
        Merge segments of the same speaker that are close in time, handling fragmented speech.
        
        Args:
            segments: List of segment dictionaries with 'start', 'end', 'text', 'channel' keys
            
        Returns:
            List of merged segments with 'raw_parts' tracking original segments
        """
        if not segments:
            return []
        
        from collections import defaultdict
        
        # Group segments by speaker
        speaker_segments = defaultdict(list)
        for seg in segments:
            speaker_segments[seg.get("channel", "Unknown")].append(seg)
        
        # Process each speaker's segments separately
        all_merged = []
        
        for speaker, speaker_segs in speaker_segments.items():
            # Sort speaker's segments by start time
            speaker_segs.sort(key=lambda x: x.get("start", 0))
            
            # Merge segments for this speaker
            merged_speaker_segs = []
            
            for seg in speaker_segs:
                # Check if we can merge with the last segment of this speaker
                if merged_speaker_segs:
                    last_seg = merged_speaker_segs[-1]
                    gap = seg.get("start", 0) - last_seg.get("end", 0)
                    
                    # Merge if gap is small (including 0.000s gaps and overlaps)
                    if gap <= GAP_TOL:
                        # Merge with previous segment
                        merged_speaker_segs[-1]["end"] = max(seg.get("end", 0), last_seg.get("end", 0))
                        merged_speaker_segs[-1]["text"] += " " + seg.get("text", "")
                        merged_speaker_segs[-1]["raw_parts"].append(seg.get("text", ""))
                        
                        # Merge words if they exist
                        if "words" in seg and "words" in merged_speaker_segs[-1]:
                            merged_speaker_segs[-1]["words"].extend(seg.get("words", []))
                        
                        continue
                
                # Create new segment
                new_seg = seg.copy()
                new_seg["raw_parts"] = [seg.get("text", "")]
                merged_speaker_segs.append(new_seg)
            
            all_merged.extend(merged_speaker_segs)
        
        # Sort all merged segments by start time
        all_merged.sort(key=lambda x: x.get("start", 0))
        
        return all_merged
    
    
    
    def process(self, segments: List[Dict]) -> List[Dict]:
        """
        Process segments by merging and cleaning text.
        
        Args:
            segments: List of segment dictionaries
            
        Returns:
            List of processed segments with merged and cleaned text
        """
        if not segments:
            return []
            
        # First merge segments
        merged_segments = self.merge_segments(segments)
        
        # Then clean the text in each segment
        # for seg in merged_segments:
        #     if "text" in seg:
        #         seg["text"] = self.clean_text(seg["text"])
        
        return merged_segments
import argparse
import os
from pydub import AudioSegment

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Restore original speed for WAV files previously slowed down.")
parser.add_argument("--input_dir", required=True, help="Directory containing slowed WAV files")
parser.add_argument("--output_dir", required=True, help="Directory to save restored audio files")
parser.add_argument("--slow_factor", type=float, default=0.75, help="The slowdown factor used earlier (e.g., 0.75 means 75%% speed)")
args = parser.parse_args()

# Ensure output directory exists
os.makedirs(args.output_dir, exist_ok=True)

# Calculate restoration speed factor
speed_factor = 1 / args.slow_factor
print(f"Restoring speed with factor: {speed_factor:.3f}x")

# Process each WAV file in the input directory
for filename in os.listdir(args.input_dir):
    if filename.lower().endswith(".wav"):
        input_path = os.path.join(args.input_dir, filename)
        print(f"Processing: {input_path}")

        # Load audio
        audio = AudioSegment.from_file(input_path)

        # Restore speed
        restored_audio = audio._spawn(audio.raw_data, overrides={
            "frame_rate": int(audio.frame_rate * speed_factor)
        }).set_frame_rate(audio.frame_rate)

        # Save restored audio
        name, ext = os.path.splitext(filename)
        output_path = os.path.join(args.output_dir, f"{name}_restored{ext}")
        restored_audio.export(output_path, format="wav")
        print(f"Saved restored audio to: {output_path}")

print("All files processed!")

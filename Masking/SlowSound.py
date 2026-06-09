import argparse
import os
from pydub import AudioSegment

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Slow down all WAV files in a folder by 0.75x speed.")
parser.add_argument("--input_dir", help="Directory containing input WAV files")
parser.add_argument("--output_dir", help="Directory to save slowed audio files")
args = parser.parse_args()

# Ensure output directory exists
os.makedirs(args.output_dir, exist_ok=True)

# Process each WAV file in the input directory
for filename in os.listdir(args.input_dir):
    if filename.lower().endswith(".wav"):
        input_path = os.path.join(args.input_dir, filename)
        print(f"Processing: {input_path}")

        # Load audio
        audio = AudioSegment.from_file(input_path)

        # Slow down by changing frame rate
        slow_audio = audio._spawn(audio.raw_data, overrides={
            "frame_rate": int(audio.frame_rate * 0.90)
        }).set_frame_rate(audio.frame_rate)

        # Save slowed audio
        name, ext = os.path.splitext(filename)
        output_path = os.path.join(args.output_dir, f"{name}_slow_90{ext}")
        slow_audio.export(output_path, format="wav")
        print(f"Saved slowed audio to: {output_path}")

print("All files processed!")

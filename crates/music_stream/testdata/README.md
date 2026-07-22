# Test media corpus

These base64 files are tiny, deterministic test-only audio fixtures. They contain generated sine
waves and no third-party program material. The current files were produced with FFmpeg 8.1.2:

```sh
ffmpeg -f lavfi -i sine=frequency=523.25:sample_rate=44100:duration=0.25 \
  -ac 1 -c:a libmp3lame -b:a 32k -write_xing 0 sine-mono.mp3
ffmpeg -f lavfi -i sine=frequency=523.25:sample_rate=44100:duration=0.2 \
  -ac 1 -c:a flac -compression_level 8 sine-mono.flac
metaflac --remove --block-type=PADDING --dont-use-padding sine-mono.flac
ffmpeg -f lavfi -i sine=frequency=523.25:sample_rate=44100:duration=0.25 \
  -ac 1 -c:a libvorbis -q:a 0 sine-mono-vorbis.ogg
ffmpeg -f lavfi -i sine=frequency=659.25:sample_rate=44100:duration=0.2 \
  -ac 1 -c:a alac -movflags +faststart sine-mono-alac.m4a
ffmpeg -f lavfi -i sine=frequency=440:sample_rate=44100:duration=0.5 \
  -ac 1 -c:a aac -b:a 24k -movflags +faststart faststart-aac.m4a
```

Each result is stored as standard base64 so the repository remains text-only. Tests strip ASCII
whitespace before decoding, allowing long fixtures to be line-wrapped.

Lege hier deine Piper-Stimme fuer den Bot ab.

Der Bot erwartet pro Stimme 2 Dateien:
- eine .onnx Datei
- die passende .onnx.json Datei

Empfohlener Start fuer Deutsch:
- de_DE-karlsson-low.onnx
- de_DE-karlsson-low.onnx.json

Beispielpfade:
- shorts_assets/voices/piper/de_DE-karlsson-low.onnx
- shorts_assets/voices/piper/de_DE-karlsson-low.onnx.json

Danach in Railway setzen:
- TTS_ENGINE=piper
- PIPER_MODEL_PATH=shorts_assets/voices/piper/de_DE-karlsson-low.onnx
- PIPER_CONFIG_PATH=shorts_assets/voices/piper/de_DE-karlsson-low.onnx.json
- PIPER_DATA_DIR=shorts_assets/voices/piper

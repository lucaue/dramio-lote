import json
import math
import struct
import tempfile
import unittest
import wave
from pathlib import Path

import lote_gh


ROOT = Path(__file__).resolve().parent


class AudioSourceSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = json.loads((ROOT / "matriz_rapida.json").read_text(encoding="utf-8"))

    def test_subtitled_editions_do_not_become_audio_tracks(self):
        book = "6a291f7b53df79f388040431"
        self.assertEqual(
            lote_gh.fontes_audio_da_matriz(book, self.matrix[book]),
            {"en": "6a18c2f9d083d51c99070fb9"},
        )

    def test_only_real_dubs_are_kept_when_original_edition_is_absent(self):
        book = "683e5a1d9bdaeff8a8074423"
        self.assertEqual(
            set(lote_gh.fontes_audio_da_matriz(book, self.matrix[book])),
            {"pt", "es", "id"},
        )

    def test_audio_signature_distinguishes_content(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.wav"
            copy = Path(directory) / "copy.wav"
            other = Path(directory) / "other.wav"
            self._write_tone(first, 440)
            self._write_tone(copy, 440)
            self._write_tone(other, 660)
            first_signature = lote_gh.assinatura_audio(str(first))
            self.assertEqual(first_signature, lote_gh.assinatura_audio(str(copy)))
            self.assertNotEqual(first_signature, lote_gh.assinatura_audio(str(other)))

    @staticmethod
    def _write_tone(path, frequency):
        sample_rate = 8000
        frames = 52 * sample_rate
        with wave.open(str(path), "wb") as target:
            target.setnchannels(1)
            target.setsampwidth(2)
            target.setframerate(sample_rate)
            chunk = bytearray()
            for index in range(frames):
                value = int(12000 * math.sin(2 * math.pi * frequency * index / sample_rate))
                chunk.extend(struct.pack("<h", value))
            target.writeframes(chunk)


if __name__ == "__main__":
    unittest.main()

"""
Generate pasangan kunci VAPID untuk Web Push GeoSense.

Pakai:
    python generate_vapid.py
    python generate_vapid.py nama-file.pem

Output:
    - file private key (default: vapid_private.pem), JANGAN di-commit
    - VAPID_PUBLIC_KEY yang harus ditaruh di .env
"""

import base64
import os
import sys

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


def main() -> None:
    output_path = sys.argv[1] if len(sys.argv) > 1 else "vapid_private.pem"
    output_path = os.path.abspath(output_path)

    if os.path.exists(output_path):
        print(f"File sudah ada: {output_path}")
        print("Hapus/rename file lama kalau memang ingin membuat kunci baru.")
        sys.exit(1)

    private_key = ec.generate_private_key(ec.SECP256R1())
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    with open(output_path, "wb") as file:
        file.write(pem)

    public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )
    public_key = base64.urlsafe_b64encode(public_bytes).rstrip(b"=").decode()

    print(f"Private key disimpan di: {output_path}")
    print("Tambahkan/update di .env:")
    print("PUSH_ENABLED=1")
    print(f"VAPID_PUBLIC_KEY={public_key}")
    print(f"VAPID_PRIVATE_KEY_PATH={os.path.basename(output_path)}")
    print("VAPID_SUBJECT=mailto:emailkamu@gmail.com")
    print()
    print("Setelah mengganti kunci, semua perangkat harus aktifkan notifikasi ulang.")


if __name__ == "__main__":
    main()

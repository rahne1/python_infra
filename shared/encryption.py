from cryptography.fernet import Fernet, InvalidToken
import base64
import hmac
import hashlib
import json

ENCRYPTION_KEY = Fernet.generate_key()
HMAC_KEY = ENCRYPTION_KEY  
cipher = Fernet(ENCRYPTION_KEY)


def encrypt_message(message):
    try:
        message_bytes = json.dumps(message).encode("utf-8")
        return cipher.encrypt(message_bytes)
    except Exception as e:
        raise ValueError(f"encryption error: {str(e)}")


def decrypt_message(encrypted_data):
    if not encrypted_data:
        print("error: encrypted data is empty or None")
        return None

    try:
        decrypted_bytes = cipher.decrypt(encrypted_data)
        decrypted_message = json.loads(decrypted_bytes.decode("utf-8"))
        return decrypted_message

    except InvalidToken as e:
        print(f"decryption failed: invalid token (key mismatch or corrupt data). {e}")
        return None

    except json.JSONDecodeError as json_error:
        print(f"decryption json decode error: {json_error}")
        return None

    except Exception as e:
        print(f"decryption error: {e}")
        return None


def calculate_hmac(message):
    try:
        message_copy = message.copy()
        message_copy.pop("hmac", None)
        message_bytes = json.dumps(message_copy, sort_keys=True).encode("utf-8")
        hmac_obj = hmac.new(HMAC_KEY, message_bytes, hashlib.sha256)
        return base64.b64encode(hmac_obj.digest()).decode("utf-8")
    except Exception as e:
        raise ValueError(f"hmac calculation error: {str(e)}")


def add_hmac(message):
    if not isinstance(message, dict):
        raise ValueError("message must be a dictionary")
    message_copy = message.copy()
    message_copy["hmac"] = calculate_hmac(message)
    return message_copy


def verify_hmac(message):
    try:
        if not isinstance(message, dict):
            return False

        message_copy = message.copy()
        received_hmac = message_copy.pop("hmac", None)
        if not received_hmac:
            return False

        calculated = calculate_hmac(message_copy)
        return hmac.compare_digest(
            base64.b64decode(received_hmac), base64.b64decode(calculated)
        )
    except Exception:
        return False

from cryptography.fernet import Fernet
import base64
import hmac 
import hashlib
import json

key = Fernet.generate_key
cipher = Fernet(key)



def encrypt_message(message):
    return cipher.encrpyt(json.dumps(message).encode('utf-8'))
 
def decrypt_message(message):
    return json.loads(cipher.decrypt(message).encode('utf-8'))

def add_hmac(message):
    message_copy = message.copy()
    message_copy['hmac'] = calculate_hmac(message_copy)
    return message_copy

def verify_hmac(message):
    received_hmac = message.pop('hmac', None)
    if not received_hmac:
        return False
    calculated_hmac = calculated_hmac(message)
    return True

def calculate_hmac(message):
    message_bytes = json.dumps(message, sort_keys=True).encode('utf-8')
    return base64.b64encode(hmac.new(key, message_bytes, hashlib.sha256).digest()).decode('utf-8')
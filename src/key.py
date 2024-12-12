import secrets

# Generate a random 256-bit (32-byte) secret key
jwt_secret = secrets.token_hex(32)

print(f"Your JWT Secret Key: {jwt_secret}")

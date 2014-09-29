# Be sure to restart your server when you modify this file.

# Your secret key is used for verifying the integrity of signed cookies.
# If you change this key, all old signed cookies will become invalid!

# Make sure the secret is at least 30 characters and all random,
# no regular words or you'll be exposed to dictionary attacks.
# You can use `rake secret` to generate a secure secret key.

# Make sure your secret_key_base is kept private
# if you're sharing your code publicly.
Etl::Application.config.secret_key_base = 'e2b7dbd08f1d29163984012fa3163835e2bd8a5925efdf298af2d18342f846d2cdd96cd4127fa1f6ca021e237a63b9a99f6bc2a8311404d1f069c8565d93ec6c'

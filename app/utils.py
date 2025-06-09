
### File: app/utils.py

# import bip39
# from ed25519_hd_key import derive_path
# from stellar_sdk import Keypair
# from bip_utils import Bip39SeedGenerator, Bip32Slip10Ed25519
# from bip_utils import Bip39SeedGenerator, Bip32Slip10Ed25519, Bip44, Bip44Coins

from bip_utils import Bip39SeedGenerator, Bip32Slip10Ed25519
from stellar_sdk import Keypair

def derive_keypair(mnemonic: str) -> Keypair:
    # Generate the seed from the mnemonic
    seed_bytes = Bip39SeedGenerator(mnemonic).Generate()

    # Derive the key using BIP-44 style path for Pi Network (uses Stellar)
    path = "m/44'/314159'/0'"
    bip32_ctx = Bip32Slip10Ed25519.FromSeed(seed_bytes)
    child_key = bip32_ctx.DerivePath(path)

    private_key = child_key.PrivateKey().Raw().ToBytes()
    kp = Keypair.from_raw_ed25519_seed(private_key)
    return kp  



# def derive_keypair(mnemonic: str) -> Keypair:
#     # Generate seed from mnemonic
#     seed_bytes = Bip39SeedGenerator(mnemonic).Generate()
#     # SLIP-10 Ed25519 derivation using your Pi path
#     bip32_ctx = Bip32Slip10Ed25519.FromSeed(seed_bytes)
#     # Apply path: m/44'/314159'/0'
#     for index in [44 + Bip32Slip10Ed25519.HardenIndex(), 314159 + Bip32Slip10Ed25519.HardenIndex(), 0 + Bip32Slip10Ed25519.HardenIndex()]:
#         bip32_ctx = bip32_ctx.ChildKey(index)
#     priv_key = bip32_ctx.PrivateKey().Raw().ToBytes()
#     return Keypair.from_raw_ed25519_seed(priv_key)


# def derive_keypair(mnemonic: str) -> Keypair:
#     if not bip39.validate_mnemonic(mnemonic):
#         raise ValueError("Invalid mnemonic")
#     seed = bip39.mnemonic_to_seed(mnemonic)
#     derived = derive_path("m/44'/314159'/0'", seed)
#     return Keypair.from_raw_ed25519_seed(derived.key)



### Joshua, de hide your seed and important info as you de run this on the web, you may face cyber attack after a successful footprinting
### But sha, I will try give you better code if you tripple my money
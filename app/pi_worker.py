### File: app/pi_worker.py

import asyncio
import httpx
import time
from stellar_sdk import Server, TransactionBuilder, Network, Asset, Keypair
from stellar_sdk.exceptions import NotFoundError
from .config import HORIZON_URL, NETWORK_PASSPHRASE, MINIMUM_BALANCE, FEE_PERCENT
from .utils import derive_keypair

server = Server(HORIZON_URL)

# Adjustable delay in seconds between each batch
TRANSACTION_DELAY_SECONDS = 0.1

async def fetch_account_and_fee(kp: Keypair):
    account = await asyncio.to_thread(server.load_account, kp.public_key)
    base_fee = await asyncio.to_thread(server.fetch_base_fee)
    return account, base_fee

async def create_signed_transaction(seed: str, destination: str, amount: float):
    kp = derive_keypair(seed)

    try:
        balance_resp = await asyncio.to_thread(server.accounts().account_id(kp.public_key).call)
        native_balance = next(
            (b["balance"] for b in balance_resp["balances"] if b["asset_type"] == "native"),
            "0"
        )
        native_balance = float(native_balance)

        estimated_fee = 0.00001  # Typically the fee per transaction
        if native_balance < amount + estimated_fee + MINIMUM_BALANCE:
            return kp.public_key, None, f"Insufficient balance ({native_balance}) for amount ({amount}) + fee + reserve"
    except NotFoundError:
        return kp.public_key, None, "Account not found"
    except Exception as e:
        return kp.public_key, None, f"Error checking balance: {str(e)}"

    fee_amount = amount * FEE_PERCENT
    send_amount = round(amount - fee_amount, 7)

    try:
        account, base_fee = await fetch_account_and_fee(kp)
        tx = (
            TransactionBuilder(account, NETWORK_PASSPHRASE, base_fee)
            .add_text_memo("PI Transfer")
            .append_payment_op(destination=destination, asset=Asset.native(), amount=str(send_amount))
            .set_timeout(30)
            .build()
        )
        tx.sign(kp)
        return kp.public_key, tx.to_xdr(), None
    except Exception as e:
        return kp.public_key, None, str(e)


async def submit_transaction(xdr: str) -> str:
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(HORIZON_URL + "/transactions", data={"tx": xdr})
            try:
                data = response.json()
            except Exception as json_error:
                return f"Invalid JSON response: {json_error}"

            if response.status_code == 200 and 'hash' in data:
                return f"Success: {data['hash']}"
            elif response.status_code == 200:
                return f"Success: incomplete response {data}"
            else:
                error_detail = data.get("extras", {}).get("result_codes", {})
                return f"Failed: {error_detail}"
        except Exception as e:
            return f"HTTP Error: {str(e)}"


async def process_batch(seeds: list[str], destination: str, amount: float) -> list[str]:
    signed_tasks = [create_signed_transaction(seed, destination, amount) for seed in seeds]
    signed_results = await asyncio.gather(*signed_tasks)

    submit_tasks = [submit_transaction(xdr) if xdr else asyncio.sleep(0) for _, xdr, _ in signed_results]
    submit_results = await asyncio.gather(*submit_tasks)

    final_results = []
    for (pubkey, xdr, err), submission in zip(signed_results, submit_results):
        if err:
            final_results.append(f"Error for {pubkey[:5]}...: {err}")
        else:
            final_results.append(f"Success for {pubkey[:5]}...: {submission}")
    return final_results


async def process_all_seeds(seeds: list[str], destination: str, amount: float) -> list[str]:
    start = time.time()
    results = []

    batch_size = 6  # Process 3 transactions per batch for reliability
    for i in range(0, len(seeds), batch_size):
        batch = seeds[i:i + batch_size]
        try:
            batch_result = await process_batch(batch, destination, amount)
        except Exception as e:
            batch_result = [f"Batch {i // batch_size + 1} failed: {str(e)}"]
        results.extend(batch_result)
        await asyncio.sleep(TRANSACTION_DELAY_SECONDS)

    end = time.time()
    print(f"Processed {len(seeds)} transactions in {round(end - start, 3)}s")
    return results




# # async def submit_transaction(xdr: str) -> str:
#     try:
#         async with httpx.AsyncClient(timeout=10.0) as client:
#             response = await client.post(HORIZON_URL + "/transactions", data={"tx": xdr})
#             data = response.json()
#             if response.status_code == 200:
#                 return f"Success: {data.get('hash', 'no hash')}"
#             else:
#                 error_detail = data.get("extras", {}).get("result_codes", "No details")
#                 return f"Failed: {error_detail}"
#     except Exception as e:
#         return f"HTTP error: {str(e)}"
    

# async def process_all_seeds(seeds: List[str], destination: str, amount: float) -> List[str]:
    start_time = time.perf_counter()

    # Step 1: Build and sign all transactions concurrently
    signed_results = await asyncio.gather(*[
        create_signed_transaction(seed, destination, amount) for seed in seeds
    ])

    # Step 2: Submit all XDRs concurrently
    submit_results = await asyncio.gather(*[
        submit_transaction(xdr) if xdr else asyncio.sleep(0) for _, xdr, _ in signed_results
    ])

    # Step 3: Build result report
    results = []
    for (pubkey, xdr, err), submission in zip(signed_results, submit_results):
        if err:
            results.append(f"Error for {pubkey[:5]}...: {err}")
        else:
            results.append(f"Success for {pubkey[:5]}...: {submission}")

    end_time = time.perf_counter()
    print(f"⚡️ Completed {len(seeds)} transfers in {round(end_time - start_time, 3)}s")

    return results


# async def process_all_seeds(seeds: List[str], destination: str, amount: float) -> List[str]:
    results = []
    try:
        start_time = time.time()

        # Split into batches
        for i in range(0, len(seeds), BATCH_SIZE):
            batch = seeds[i:i + BATCH_SIZE]

            print(f"📦 Processing batch {i // BATCH_SIZE + 1} with {len(batch)} seeds")

            # Step 1: Create transactions
            signed_results = await asyncio.gather(*[
                create_signed_transaction(seed, destination, amount) for seed in batch
            ])

            # Step 2: Submit transactions
            submit_results = await asyncio.gather(*[
                submit_transaction(xdr) if xdr else asyncio.sleep(0) for _, xdr, _ in signed_results
            ])

            # Step 3: Format results
            for (pubkey, xdr, err), submission in zip(signed_results, submit_results):
                if err:
                    results.append(f"Error for {pubkey[:5]}...: {err}")
                else:
                    results.append(f"Success for {pubkey[:5]}...: {submission}")

            # Delay between batches
            await asyncio.sleep(BATCH_DELAY_SECONDS)

        total_time = round(time.time() - start_time, 3)
        print(f"✅ Processed {len(seeds)} transactions in {total_time}s")

        return results

    except Exception as e:
        print("🔥 Error in process_all_seeds:", str(e))
        traceback.print_exc()
        return [f"Server error: {str(e)}"]


# import asyncio
# import httpx
# import time
# from stellar_sdk import Server, TransactionBuilder, Network, Asset, Keypair
# from stellar_sdk.exceptions import NotFoundError
# from .config import HORIZON_URL, NETWORK_PASSPHRASE, MINIMUM_BALANCE, FEE_PERCENT
# from .utils import derive_keypair

# server = Server(HORIZON_URL)


# async def fetch_account_and_fee(kp: Keypair):
#     account = await server.load_account(kp.public_key)
#     base_fee = await server.fetch_base_fee()
#     return account, base_fee


# async def create_signed_transaction(seed: str, destination: str, amount: float):
#     kp = derive_keypair(seed)

#     try:
#         balance_resp = await server.accounts().account_id(kp.public_key).call()
#         native_balance = next(
#             (b["balance"] for b in balance_resp["balances"] if b["asset_type"] == "native"),
#             "0"
#         )
#         if float(native_balance) < MINIMUM_BALANCE:
#             return kp.public_key, None, f"Insufficient balance: {native_balance}"
#     except NotFoundError:
#         return kp.public_key, None, "Account not found"

#     fee_amount = amount * FEE_PERCENT
#     send_amount = round(amount - fee_amount, 7)

#     try:
#         account, base_fee = await fetch_account_and_fee(kp)
#         tx = (
#             TransactionBuilder(account, NETWORK_PASSPHRASE, base_fee)
#             .add_text_memo("PI Transfer")
#             .append_payment_op(destination=destination, asset=Asset.native(), amount=str(send_amount))
#             .set_timeout(30)
#             .build()
#         )
#         tx.sign(kp)
#         return kp.public_key, tx.to_xdr(), None
#     except Exception as e:
#         return kp.public_key, None, str(e)


# async def submit_transaction(xdr: str):
#     async with httpx.AsyncClient(timeout=5.0) as client:
#         try:
#             response = await client.post(HORIZON_URL + "/transactions", data={"tx": xdr})
#             data = response.json()
#             if response.status_code == 200:
#                 return f"Success: {data['hash']}"
#             else:
#                 error_detail = data.get("extras", {}).get("result_codes", {})
#                 return f"Failed: {error_detail}"
#         except Exception as e:
#             return f"HTTP Error: {str(e)}"


# async def process_all_seeds(seeds: list[str], destination: str, amount: float) -> list[str]:
#     start = time.time()
#     results = []

#     # Step 1: Create all signed XDR transactions in parallel
#     signed_tasks = [create_signed_transaction(seed, destination, amount) for seed in seeds]
#     signed_results = await asyncio.gather(*signed_tasks)

#     # Step 2: Submit XDRs in parallel
#     submit_tasks = [submit_transaction(xdr) if xdr else asyncio.sleep(0) for _, xdr, err in signed_results]
#     submit_results = await asyncio.gather(*submit_tasks)

#     # Step 3: Merge and format results
#     for (pubkey, xdr, err), submission in zip(signed_results, submit_results):
#         if err:
#             results.append(f"Error for {pubkey[:5]}...: {err}")
#         else:
#             results.append(f"Success for {pubkey[:5]}...: {submission}")

#     end = time.time()
#     print(f"Processed {len(seeds)} transactions in {round(end - start, 3)}s")
#     return results




# import asyncio
# from stellar_sdk import Server, TransactionBuilder, Network, Asset
# from stellar_sdk.exceptions import NotFoundError
# from .config import HORIZON_URL, NETWORK_PASSPHRASE, MINIMUM_BALANCE, FEE_PERCENT
# from .utils import derive_keypair

# server = Server(HORIZON_URL)

# # Synchronous: Get balance of a Stellar account
# def get_balance(public_key: str) -> float:
#     try:
#         account = server.accounts().account_id(public_key).call()
#         balance = next(
#             (b["balance"] for b in account["balances"] if b["asset_type"] == "native"),
#             "0",
#         )
#         return float(balance)
#     except NotFoundError:
#         return 0.0

# # Synchronous: Transfer PI from one keypair to a destination

# def transfer_pi(seed: str, destination: str, amount: float) -> str:
#     kp = derive_keypair(seed)
#     balance = get_balance(kp.public_key)

#     if balance < MINIMUM_BALANCE:
#         return f"Seed {kp.public_key} has insufficient balance: {balance} PI"

#     try:
#         fee_amount = amount * FEE_PERCENT
#         send_amount = round(amount - fee_amount, 7)

#         account = server.load_account(kp.public_key)
#         base_fee = server.fetch_base_fee()

#         tx = (
#             TransactionBuilder(source_account=account, network_passphrase=NETWORK_PASSPHRASE, base_fee=base_fee)
#             .add_text_memo("PI Transfer")
#             .append_payment_op(destination=destination, amount=str(send_amount), asset=Asset.native())
#             .set_timeout(30)
#             .build()
#         )

#         tx.sign(kp)
#         response = server.submit_transaction(tx)

#         # Safely get transaction hash
#         tx_hash = response.get("hash")
#         if tx_hash:
#             return f"Success for {kp.public_key}: {tx_hash}"
#         else:
#             return f"Error for {kp.public_key}: No hash returned. Full response: {response}"

#     except Exception as e:
#         # Attempt to extract `result_codes` from the response if available
#         if hasattr(e, 'response') and isinstance(e.response, dict):
#             extras = e.response.get("extras", {})
#             result_codes = extras.get("result_codes", {})
#             return (
#                 f"Error for {kp.public_key}: Transaction failed. "
#                 f"Codes: {result_codes}. Full: {extras}"
#             )
#         return f"Error for {kp.public_key}: {str(e)}"

# def transfer_pi(seed: str, destination: str, amount: float) -> str:
#     kp = derive_keypair(seed)
#     balance = get_balance(kp.public_key)

#     if balance < MINIMUM_BALANCE:
#         return f"Seed {kp.public_key} has insufficient balance: {balance} PI"

#     try:
#         fee_amount = amount * FEE_PERCENT
#         send_amount = round(amount - fee_amount, 7)

#         account = server.load_account(kp.public_key)
#         base_fee = server.fetch_base_fee()

#         tx = (
#             TransactionBuilder(source_account=account, network_passphrase=NETWORK_PASSPHRASE, base_fee=base_fee)
#             .add_text_memo("PI Transfer")
#             .append_payment_op(destination=destination, amount=str(send_amount), asset=Asset.native())
#             .set_timeout(30)
#             .build()
#         )

#         tx.sign(kp)
#         response = server.submit_transaction(tx)

#         # Safely extract transaction hash
#         tx_hash = response.get("hash")
#         if tx_hash:
#             return f"Success for {kp.public_key}: {tx_hash}"
#         else:
#             return f"Error for {kp.public_key}: Transaction submitted, but no hash returned. Response: {response}"

#     except Exception as e:
#         return f"Error for {kp.public_key}: {str(e)}"


# Asynchronous: Process multiple seeds in parallel using asyncio.to_thread
# async def process_all_seeds(seeds: list[str], destination: str, amount: float) -> list[str]:
#     async def safe_transfer(seed: str):
#         try:
#             return await asyncio.to_thread(transfer_pi, seed, destination, amount)
#         except Exception as e:
#             return f"Error for seed {seed[:5]}...: {str(e)}"

#     tasks = [safe_transfer(seed) for seed in seeds]
#     return await asyncio.gather(*tasks)


# from stellar_sdk import Server, TransactionBuilder, Network, Asset
# from stellar_sdk.exceptions import NotFoundError
# from .config import HORIZON_URL, NETWORK_PASSPHRASE, MINIMUM_BALANCE, FEE_PERCENT
# from .utils import derive_keypair

# import asyncio

# server = Server(HORIZON_URL)


# async def get_balance(public_key: str) -> float:
#     try:
#         account = await server.accounts().account_id(public_key).call()
#         balance = next(
#             (b["balance"] for b in account["balances"] if b["asset_type"] == "native"),
#             "0",
#         )
#         return float(balance)
#     except NotFoundError:
#         return 0.0


# async def transfer_pi(seed: str, destination: str, amount: float) -> str:
#     kp = derive_keypair(seed)
#     balance = await get_balance(kp.public_key)

#     if balance < MINIMUM_BALANCE:
#         return f"Seed {kp.public_key} has insufficient balance: {balance} PI"

#     fee_amount = amount * FEE_PERCENT
#     send_amount = round(amount - fee_amount, 7)

#     account = await server.load_account(kp.public_key)
#     base_fee = await server.fetch_base_fee()

#     tx = (
#         TransactionBuilder(source_account=account, network_passphrase=NETWORK_PASSPHRASE, base_fee=base_fee)
#         .add_text_memo("PI Transfer")
#         .append_payment_op(destination=destination, amount=str(send_amount), asset=Asset.native())
#         .set_timeout(30)
#         .build()
#     )

#     tx.sign(kp)
#     response = await server.submit_transaction(tx)
#     return f"Success for {kp.public_key}: {response['hash']}"


# async def process_all_seeds(seeds: list[str], destination: str, amount: float) -> list[str]:
#     async def safe_transfer(seed: str):
#         try:
#             return await transfer_pi(seed, destination, amount)
#         except Exception as e:
#             return f"Error for seed {seed[:5]}...: {str(e)}"

#     tasks = [safe_transfer(seed) for seed in seeds]
#     return await asyncio.gather(*tasks)



# import asyncio
# from stellar_sdk import Server, TransactionBuilder, Network, Asset, Operation
# from stellar_sdk.exceptions import NotFoundError
# from .config import HORIZON_URL, NETWORK_PASSPHRASE, MINIMUM_BALANCE, FEE_PERCENT
# from .utils import derive_keypair

# server = Server(HORIZON_URL)


# async def get_balance(public_key: str) -> float:
#     try:
#         account = await server.accounts().account_id(public_key).call()
#         balance = next(
#             (b["balance"] for b in account["balances"] if b["asset_type"] == "native"),
#             "0",
#         )
#         return float(balance)
#     except NotFoundError:
#         return 0.0


# async def transfer_pi(seed: str, destination: str, amount: float) -> str:
#     kp = derive_keypair(seed)
#     balance = await get_balance(kp.public_key)

#     if balance < MINIMUM_BALANCE:
#         return f"Seed {kp.public_key} has insufficient balance: {balance} PI"

#     fee_amount = amount * FEE_PERCENT
#     send_amount = round(amount - fee_amount, 7)

#     account = await server.load_account(kp.public_key)
#     base_fee = await server.fetch_base_fee()

#     tx = (
#         TransactionBuilder(account, NETWORK_PASSPHRASE, base_fee)
#         .add_text_memo("PI Transfer")
#         .add_operation(
#             Operation.payment(destination=destination, asset=Asset.native(), amount=str(send_amount))
#         )
#         .set_timeout(30)
#         .build()
#     )

#     tx.sign(kp)
#     response = await server.submit_transaction(tx)
#     return f"Success for {kp.public_key}: {response['hash']}"


# async def process_all_seeds(seeds: list[str], destination: str, amount: float) -> list[str]:
#     results = []
#     for seed in seeds:
#         try:
#             result = await transfer_pi(seed, destination, amount)
#         except Exception as e:
#             result = f"Error for seed: {str(e)}"
#         results.append(result)
#         await asyncio.sleep(0.005)  # 5ms between each to avoid rate limits
#     return results

# See ba, I de avoid those network latency issues


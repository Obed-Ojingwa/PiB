# from app.utils import send_transaction_batch
# import asyncio

# # Controller for continuous background processing
# running = False

# def start_continuous():
#     global running
#     running = True

# def stop_continuous():
#     global running
#     running = False

# def is_running():
#     return running

# async def process_all_seeds():
#     from app.config import seeds, recipients, amount, memo, batch_size

#     total = len(seeds)
#     results = []
#     print(f"Processing {total} transactions in batches of {batch_size}")

#     for i in range(0, total, batch_size):
#         if not running:
#             print("Stopped by user")
#             break

#         seed_batch = seeds[i:i + batch_size]
#         to_batch = recipients[i:i + batch_size]

#         print(f"Processing batch {i // batch_size + 1}...")
#         batch_results = await send_transaction_batch(seed_batch, to_batch, amount, memo)
#         results.extend(batch_results)

#         await asyncio.sleep(1)  # Optional delay to control rate

#     print("Finished processing.")
#     return results

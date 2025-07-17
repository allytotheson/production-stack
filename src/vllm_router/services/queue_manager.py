import asyncio
import logging

logger = logging.getLogger(__name__)

class RouterQueueManager:
    def __init__(self, max_queue_size=200):
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.running = False

    async def enqueue(self, request_data):
        try:
            await self.queue.put(request_data)
        except asyncio.QueueFull:
            logger.warning("Queue is full. Dropping request.")
            raise

    async def start_worker(self, handler_fn):
        self.running = True
        logger.info("Router queue worker started.")
        while self.running:
            try:
                request_data = await self.queue.get()
                await handler_fn(**request_data)
            except Exception as e:
                logger.error(f"Error processing queued request: {e}", exc_info=True)

    def stop(self):
        self.running = False

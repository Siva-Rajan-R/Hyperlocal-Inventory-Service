from .main import RabbitMQMessagingConfig,ExchangeType
from .controllers.controller import ConsumersHandler
import asyncio

async def worker():
    rabbitmq_conn=await RabbitMQMessagingConfig.get_rabbitmq_connection()
    rabbitmq_msg_obj=RabbitMQMessagingConfig(rabbitMQ_connection=rabbitmq_conn)

    # Exchanges
    exchanges=[
        {'name':'products.inventory.inventory.exchange','exc_type':ExchangeType.TOPIC},
        {'name':'shops.inventory.inventory.exchange','exc_type':ExchangeType.TOPIC},
        {'name':'products.purchase.purchase.exchange','exc_type':ExchangeType.TOPIC},
        {'name':'suppliers.purchase.purchase.exchange','exc_type':ExchangeType.TOPIC}

    ]

    for exchange in exchanges:
        await rabbitmq_msg_obj.create_exchange(name=exchange['name'],exchange_type=exchange['exc_type'])

    # Queues
    queues=[
        {'exc_name':'products.inventory.inventory.exchange','q_name':'products.inventory.inventory.queue','r_key':'products.inventory.*.*.v1'},
        {'exc_name':'shops.inventory.inventory.exchange','q_name':'shops.inventory.inventory.queue','r_key':'shops.inventory.*.*.v1'},
        {'exc_name':'products.purchase.purchase.exchange','q_name':'products.purchase.purchase.queue','r_key':'products.purchase.*.*.v1'},
        {'exc_name':'suppliers.purchase.purchase.exchange','q_name':'suppliers.purchase.purchase.queue','r_key':'suppliers.purchase.*.*.v1'}
    ]

    for queue in queues:
        queue=await rabbitmq_msg_obj.create_queue(
            exchange_name=queue['exc_name'],
            queue_name=queue['q_name'],
            routing_key=queue['r_key']
        )

    # Consumers
    consumers=[
        {'q_name':'products.inventory.inventory.queue','handler':ConsumersHandler.main_handler},
        {'q_name':'shops.inventory.inventory.queue','handler':ConsumersHandler.main_handler},
        {'q_name':'products.purchase.purchase.queue','handler':ConsumersHandler.main_handler},
        {'q_name':'suppliers.purchase.purchase.queue','handler':ConsumersHandler.main_handler}
    ]

    for consumer in consumers:
        await rabbitmq_msg_obj.consume_event(queue_name=consumer['q_name'],handler=consumer['handler'])

    await asyncio.Event().wait()

    



    
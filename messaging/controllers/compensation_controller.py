from aio_pika.abc import AbstractIncomingMessage
from hyperlocal_platform.core.enums.routingkey_enum import RoutingkeyActions
from icecream import ic
from dataclasses import dataclass


@dataclass(frozen=True)
class CompensationController:
    msg:AbstractIncomingMessage

    def decide(self,saga_payload:dict)->bool:
        """This method will proccess and decide the compenstaion for the controller
        based on the incoming message,and the payload of saga.
        """
        routing_key:str=self.msg.routing_key
        domain,work_for,action,state,version=routing_key.split('.')

        key:str=f"{work_for}.{action}"

        ic(key,saga_payload,work_for=='products' and key in [f"products.{RoutingkeyActions.CREATE.value}"])
        if work_for=='products' and key in [f"products.{RoutingkeyActions.CREATE.value}"]:
            ic("saga payload : ",saga_payload['data'].get("products").get('is_new'))
            if saga_payload['data'].get("products").get('is_new'):
                ic(True)
                return True
        
        return False
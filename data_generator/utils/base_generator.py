import random
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
from typing import Any
from data_generator.config.settings import settings
from data_generator.customer.profile_builder import CustomerProfile
from data_generator.utils.dirty_data import DirtyDataInjector
from data_generator.utils.logger import get_logger

record =dict[str, Any]

def random_timestamp(days_back: int=180,rng: random.Random=random.Random ) -> str:
    rng = rng or random
    delts = timedelta(
        days=rng.randint(0,days_back),
        hours=rng.randint(0,23),
        minutes=rng.randint(0,59),
        seconds=rng.randint(0,59),
    )
    ts = datetime.now(tz=timezone.utc) - delts
    return ts.isoformat()

def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"

class BaseGenerator(ABC):
    source_name: str ='base'

    def __init__(self,customers: list[CustomerProfile],seed: int | None = None , dirty_rate: float = settings.dirty_date_rate) -> None:
        if not customers:
            raise ValueError("At least one customer profile is requird to generate transactions")
        self.customers = customers
        self.rng = random.Random(seed)
        self.injector= DirtyDataInjector(rate=dirty_rate,seed=seed)
        self.logger=get_logger(self.__class__.__name__)


    def _random_customer(self) -> CustomerProfile:
        return self.rng.choice(self.customers)

    @abstractmethod
    def _build_record(self) -> record:
        raise NotImplementedError()

    def generate_one(self) -> record:
        record = self._build_record()
        record['source']=self.source_name
        return self.injector.maybe_corrupt(record)

    def generate(self,count:int , max_workers: int=settings.max_worker_threads) -> list[record]:
        if count <= 2000:
            return [self.generate_one() for _ in range(count)]

        self.logger.info("Generating %s %s records with %s threads",count,self.source_name,max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results =list(pool.map(lambda _: self.generate_one(),range(count)))
            return results

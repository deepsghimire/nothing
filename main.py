import dataclasses
import itertools
import logging
import struct
from dataclasses import dataclass
from pprint import pprint

import redis
from redis.commands.search.field import NumericField, TagField, VectorField
from redis.commands.search.query import Query

logging.basicConfig(level=logging.INFO)


@dataclass
class Ticket:
    skill: float
    sigma: float
    completion_ratio: float
    resp_time: float

    def tobytes(self) -> bytes:
        return struct.pack("dddd", *dataclasses.astuple(self))

    @staticmethod
    def frombytes(bytes):
        return Ticket(*struct.unpack("dddd", bytes))


tickets = [
    Ticket(100, 1, 0.8, 10),
    Ticket(200, 2, 0.9, 5),
    Ticket(300, 0.5, 1, 6),
    Ticket(400, 1.5, 0.1, 7),
    Ticket(500, 2, 1, 10),
    Ticket(600, 3, 1, 40),
    Ticket(700, 4, 0.8, 20),
    Ticket(800, 1, 0.7, 30),
]


def get_redis_connection(host="localhost", port=6379):
    return redis.Redis(host=host, port=port)


vectors = len(tickets)
dim = 4
k = 4


def load_data(client, data):
    for (index, value) in enumerate(data):
        client.hset(index, mapping={"skill": value.tobytes()})
    logging.info("data loaded")


skills = range(100, 1000, 100)
sigmas = range(-5, 5, 1)
comps = [1 / (1 + x) for x in range(10)]
resps = range(10, 50)


def load_data2(client):
    for (index, (skill, sigma, comp, resp)) in enumerate(
        itertools.product(skills, sigmas, comps, resps)
    ):
        client.hset(
            index, mapping={"skill": Ticket(skill, sigma, comp, resp).tobytes()}
        )


def print_res(res):
    for each in res.items():
        print(each)


def build_schema(client):
    schema = VectorField(
        "skill", "FLAT", {"TYPE": "FLOAT64", "DIM": dim, "DISTANCE_METRIC": "L2"}
    )
    client.ft().create_index(schema)
    client.ft().config_set("default_dialect", 2)
    logging.info("Index built")


def delete_data(client):
    client.flushall()


def find_match(ticket, client=get_redis_connection()):
    q = Query("* => [KNN 4 @skill $vec]")
    result = []
    for each in client.ft().search(q, query_params={"vec": ticket.tobytes()}).docs:
        skills = client.hget(each.id, "skill")
        result.append(Ticket.frombytes(skills))
    return result


def main():
    client = get_redis_connection()
    delete_data(client)
    build_schema(client)
    load_data2(client)
    logging.info(f"Index size: {client.ft().info()['num_docs']}")
    input()
    for (skill, sigma, comp, resp) in itertools.product(skills, sigmas, comps, resps):
        each = Ticket(skill, sigma, comp, resp)
        print(f"for {each}:")
        pprint(find_match(each, client))
        print()

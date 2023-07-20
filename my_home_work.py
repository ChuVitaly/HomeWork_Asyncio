import asyncio
import datetime
import aiohttp
import pony.orm as pny
from pony.orm import Required, db_session, Optional, Json
from pony.orm.core import commit

database = pny.Database("sqlite", "personage.sqlite", create_db=True)


class Personage(database.Entity):
    birth_year = Required(str)
    eye_color = Required(str)
    films = Required(str)
    gender = Required(str)
    hair_color = Required(str)
    height = Required(str)
    homeworld = Required(str)
    mass = Required(str)
    name = Required(str)
    skin_color = Required(str)
    species = Optional(Json)
    starships = Optional(Json)
    vehicles = Optional(Json)


pny.sql_debug(True)

database.generate_mapping(create_tables=True)


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await response.json()
    await session.close()
    return json_data


async def main():
    person_1_coro = get_people(1)
    person_2_coro = get_people(2)
    person_3_coro = get_people(3)
    person_4_coro = get_people(4)

    persons = await asyncio.gather(person_1_coro, person_2_coro, person_3_coro, person_4_coro)

    with db_session:
        for person in persons:
            films = ', '.join(person['films'])
            species = ', '.join(person['species']) if person['species'] else ""
            Personage(birth_year=person.get('birth_year', ''),
                      eye_color=person.get('eye_color', ''),
                      films=films,
                      gender=person.get('gender', ''),
                      hair_color=person.get('hair_color', ''),
                      height=person.get('height', ''),
                      homeworld=person.get('homeworld', ''),
                      mass=person.get('mass', ''),
                      name=person.get('name', ''),
                      skin_color=person.get('skin_color', ''),
                      species=species,
                      starships=person.get('starships', ''),
                      vehicles=person.get('vehicles', ''))

        commit()


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)

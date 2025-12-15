from faker import Faker
from dataclasses import dataclass
from datetime import datetime
import random
from abc import ABC, abstractmethod
import pandas as pd
import os


@dataclass
class userevent:
    # creates dummy data for iceberg update testing
    # to calculate the time taken for the update
    id: int
    name: str
    email: str
    city: str
    event: str
    event_datetime: datetime


class path:
    # to get current path file details
    def __init__(self):
        self.current_path = os.getcwd()

    def read_file_path(self, filename):
        return os.path.join(self.current_path, filename)


class basedatagenerator(ABC):
    # generate the random data using faker
    @abstractmethod
    def generator(self):
        pass


class generatedata(basedatagenerator):
    def __init__(self):
        self.fake = Faker()
        self.Events = ['login', 'logout', 'purchase', 'cart']

    def generator(self) -> userevent:
        return userevent(
            id=random.randint(1, 100),
            name=self.fake.name(),
            email=self.fake.email(),
            city=self.fake.city(),
            event=random.choice(self.Events),
            event_datetime=self.fake.date_time_this_year()
        )


class datagenerate:
    # creates a dependency injection on the faker generator class
    def __init__(self, generator: basedatagenerator):
        self.generator = generator

    def generate(self, n: int):
        for _ in range(n):
            yield self.generator.generator()


class writer_to_csv:
    # write and update csv using the path interface class
    def __init__(self, filepath: path):
        self.filepath = filepath

    def write_csv(self, record, filename):
        data = pd.DataFrame([vars(r) for r in record])
        data.to_csv(self.filepath.read_file_path(filename), index=False)

    def write_update_csv(self, df, filename):
        df.to_csv(self.filepath.read_file_path(filename), index=False)


class csvreader:
    # read the source file
    def read(self, path):
        return pd.read_csv(path)


class event_modifier:
    # update the dataframe
    def update_event(self, df, n: int):
        df_copy = df.copy()
        last_rows = df_copy.tail(n).index
        df_copy.loc[last_rows, "event_datetime"] = datetime.now()
        return df_copy.loc[last_rows]


if __name__ == "__main__":
    source_filename = "demo.csv"
    update_filename = "update.csv"

    generator = generatedata()
    producer = datagenerate(generator)
    data = producer.generate(100)

    fullpath = path()
    filepath = fullpath.read_file_path(source_filename)

    writer = writer_to_csv(fullpath)
    writer.write_csv(data, source_filename)

    reader = csvreader()
    update = event_modifier()

    df = reader.read(filepath)
    df_update = update.update_event(df, 10)
    writer.write_update_csv(df_update, update_filename)

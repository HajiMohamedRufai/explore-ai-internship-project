"""Code to process data."""

from typing import List
import random


def generate_data(
    length: int,
    lowest: int = 0,
    highest: int = 100,
) -> List:
    """Generate data.

    A simple example to show how to separate out your code base in a clean way.

    See notebooks/0.0-example.ipynb

    Args:
        length (int): How many elements to have in the generated list.
        lowest (int, optional): Lowest possible int in the randomly generated list. Defaults to 0.
        highest (int, optional): Highest possible int in the randomly generated list. Defaults to 100.

    Returns:
        List: A list of random numbers.
    """
    return [random.randint(lowest, highest) for _ in range(length)]

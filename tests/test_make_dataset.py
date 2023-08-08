from src.data.make_dataset import generate_data


def test_make_dataset_constraints():
    """A minimal example showing how to write a unit test.

    The basic structure will be:
    - execute the function and get a result
    - assert that certain conditions have been met

    Once your project grows, having tests of the small units of "work"
    will help you debug and go faster, safer.
    """
    length = 100
    min_int = 0
    max_int = 1
    result = generate_data(
        length=length,
        lowest=min_int,
        highest=max_int,
    )

    # test the returned result is of the correct length
    assert (
        len(result) == length
    ), f"Result doesn't contain {length} elements, found {len(result)}"

    # test the generated data isn't outside of the provided bounds - lower
    assert (
        min(result) >= min_int
    ), f"Expected no results less than {min_int} found {min(result)}"

    # test the generated data isn't outside of the provided bounds - upper
    assert (
        max(result) <= max_int
    ), f"Expected no results more than {max_int} found {max(result)}"

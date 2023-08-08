# Making sense of the DALRRD Daily Commodity Prices 

## Project description

- *Include a short paragraph about the project and the code artefacts in this project.*
- *Provide any relevant external links, e.g. to Notion, your S3 bucket, etc.*

## Team Members

- (Team Lead) Onidajo_anu Fikayo Wale-Olaitan
- (Data Scientist) Busayo Iman
- (Data Scientist) Nonso Azih
- (Data Scientist) Pharis Ihaki
- (Data Scientist) Karabo Eugene
- (Data Engineer) Haji Rufai

## Environment

It's highly recommended to use a virtual environment for your project, there are many ways to do this,
below we have provided one example of how this can be achieved. Ensure when working on your project
to keep this section up-to-date so if anyone needs to run your code they know the exact steps needed
to get the appropriate environment ready. A person should be able to clone your repo and get up and
running with the instructions provided here.

### Setup - you only need to do this once

```bash
# make sure your pip in your base Python installation is up-to-date
python3 -m pip install -U pip
# install the virtualenv package
python3 -m pip install virtualenv
```

### Create the virtual environment - also typically only run when needed

```bash
# create a virtual environment in this directory called '.venv'
python3 -m venv .venv
# you should now see the folder `.venv` in your repo
```

### This is how you activate the virtual environment in a terminal and install the project dependencies

```bash
# activate the virtual environment
source .venv/bin/activate
# install the requirements for this project
pip install -r requirements.txt
```

## Tests

It's highly recommended to get in the habit of writing tests, especially once you've identified something
concrete that you can refactor into a reusable bit of code. This project has been seeded with a minimal
working example of a [refactored function](src/data/make_dataset.py),
[a notebook that can import this code](notebooks/0.0-example.ipynb) and
[a unit test to test the code behaves as expected](tests/test_make_dataset.py).

You can execute tests from the terminal by running `pytest`. IDEs like VSCode have built-in support for
executing and debugging tests through the "Testing" menu.

## Project Organisation

```ascii
├── LICENSE            <- Contains information about the legal terms and conditions under which
|                         the code can be used.
|
├── README.md          <- The top-level README for developers using this project.
│
├── docs               <- Use markdown. If/when the project becomes more complex consider migrating
|                         to something like pdoc or sphinx depending on the nature of the project.
|                         Think carefully about what documentation should sit alongside the code
|                         and what you can rather include in docstrings.
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering), and a
│                         short `-` delimited description, e.g. `1.0-initial-data-exploration`.
|                         Refactor the good parts. Don't write code to do the same task in multiple
|                         notebooks. If it's a data preprocessing task, put it in a file like
|                         `src/data/make_dataset.py`. If it's useful utility code, refactor it to
|                         `src` and import accordingly. Only commit clean notebooks (clear all cell outputs).
│
├── requirements.txt   <- The requirements file for reproducing the environment.
|
├── src                <- Source code. Refactor clean, re-usable code here.
│   │
│   ├── data           <- Separate your code base into logical groups, e.g. data, features, models, visualisation, etc.
│   │   └── make_dataset.py
│   └── ...
|
├── tests              <- Unit tests. Filename should start with "test_".
    └── test_make_dataset.py
```

# Table of contents

- [Google BigQuery Crawler](#google-bigquery-crawler)
- [Setup](#setup)
- [Get dependencies from BigQuery](#get-dependencies-from-bigquery)
- [Examples](#examples)
  - [Example 1](#example-1)
  - [Example 2](#example-2)
- [Notes](#notes)

# Google BigQuery Crawler

This crawler can be used to find information about the dependencies of all BigQuery projects that are available to you.

# Setup

1. Install the Google Cloud SDK and set it up
2. Install Python dependencies from Pipfile with `pipenv install`
3. Start the Pipenv shell with `pipenv shell`
4. Execute `main.py` with `py main.py` (or `python main.py` / `python3 main.py`)

# Get dependencies from BigQuery

After executing the script you get a prompt to enter some information for your search.

1. Enter project id: Where do you want to search? `all` means, you will search in all projects that are available for you. You can also enter a specific project id.
2. Enter the mode:
   1. `schema`: This will search through all schemas
   2. `query`: This will search through all view-queries
   3. `schquery`: This will search through all scheduled query-queries
   4. `schquerytab`: This will search through all scheduled query-destination tables
3. Search string: Enter a string you want to search for
4. Name of the result file: How should the name of the generated file be? Defaults to `result.json`. The script always generates a JSON file.

# Examples

## Example 1

As an example we want to get all scheduled queries that use the table `dataset_1.table_1` in the project `project_id_1`.

1. Enter project id: `project_id_1`
2. Enter mode: `schquery`
3. Enter search string: `dataset_1.table_1`
4. Name of result file: `table1Results.json`

The result gets generated in a JSON file called `table1Results.json` in the root directory and looks like this for this example:

```json
{
  "mode": "schquery",
  "search_string": "dataset_1.table_1",
  "results": [
    {
      "project_id": "project_id_1",
      "resources": [
        {
          "type": "SCHEDULED QUERY",
          "source": "sq_1",
          "destination": "dataset_2.t_1",
          "number_of_appearances": 1
        },
        {
          "type": "SCHEDULED QUERY",
          "source": "sq_2",
          "destination": "dataset_1.table_3",
          "number_of_appearances": 1
        }
      ]
    }
  ]
}
```

In the JSON you see the field `results`. This is the interesting part since here you can find the results of the search.
The first result is:

```json
{
  "type": "SCHEDULED QUERY",
  "source": "sq_1",
  "destination": "dataset_2.t_1",
  "number_of_appearances": 1
}
```

- `type` is the type of the resource
- `source` is the source table, view or scheduled query
- `destination` is the destination table the scheduled query writes to
- `number_of_appearances` is how often this result was found in the search process

&rarr; The scheduled query `sq_1` uses in its query the table `dataset_1.table_1` and writes the outcome of this query to the table `dataset_2.t_1`.

---

## Example 2

As a new example we want to get all views that use the table `dataset_2.table_2` in the project `project_id_1`.

1. Enter project id: `project_id_1`
2. Enter mode: `query`
3. Enter search string: `dataset_2.table_2`
4. Name of result file: `<Nothing entered>`

The generated `result.json` (Since no file name was entered):

```json
{
  "mode": "query",
  "search_string": "dataset_2.table_2",
  "results": [
    {
      "project_id": "project_id_1",
      "resources": [
        {
          "type": "VIEW",
          "source": "dataset_2.v_xyz",
          "destination": null,
          "number_of_appearances": 2
        },
        {
          "type": "VIEW",
          "source": "dataset_3.v_abc",
          "destination": null,
          "number_of_appearances": 3
        },
        {
          "type": "VIEW",
          "source": "dataset_3.v_lmn",
          "destination": null,
          "number_of_appearances": 1
        },
        {
          "type": "VIEW",
          "source": "dataset_5.v_151",
          "destination": null,
          "number_of_appearances": 2
        }
      ]
    }
  ]
}
```

The first result is:

```json
{
  "type": "VIEW",
  "source": "dataset_2.v_xyz",
  "destination": null,
  "number_of_appearances": 2
}
```

&rarr; The view `dataset_2.v_xyz` uses in its query the table `dataset_2.table_2` twice.

## Notes

In case you select all projects the resources of the projects would get separated in the `result.json`.

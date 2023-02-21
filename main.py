import json

from google.cloud import bigquery, bigquery_datatransfer
from google.protobuf.json_format import MessageToDict


def main():

    bigquery_client = bigquery.Client()

    project_ids = []

    print("(------------------------------------------------------------------------------------------------)")

    project_id_input = input("Please enter project id (or 'all'): ")
    mode = input("Please enter mode (schema, query, schquery, schquerytab): ")
    search = input("Search string: ")
    result_file_name = input("Name of the result file (defaults to 'result.json'): ").strip()

    print("(------------------------------------------------------------------------------------------------)")

    if project_id_input == "all":
        projects = list(bigquery_client.list_projects())
        for proj in projects:
            project_ids.append(proj.project_id)
    else:
        project_ids.append(project_id_input)

    json_to_write: dict = {
        "mode": mode,
        "search_string": search,
        "results": []
    }

    for project_id in project_ids:

        datasets = list(bigquery_client.list_datasets(project_id))

        if mode == "schema":
            result = search_schemas(bigquery_client, project_id, datasets, search)

        elif mode == "query":
            result = search_queries(bigquery_client, project_id, datasets, search)

        elif mode == "schquery":
            result = search_scheduled_queries(project_id, search)

        elif mode == "schquerytab":
            result = search_scheduled_queries_with_table_name(project_id, search)

        else:
            print("Invalid mode input...")
            return

        print(f"Scanned {project_id}")

        if result:
            json_to_write.get("results").append(result)

    with open(result_file_name if result_file_name else "result.json", "w") as stream:
        stream.write(json.dumps(json.loads(json.dumps(json_to_write)), indent=2, sort_keys=False))

    print("(------------------------------------------------------------------------------------------------)")
    print("Created a JSON file with the results in the execution directory.")


def search_schemas(client, project_id, datasets, search_string):

    # ------------- Start to scan table columns -------------

    column_app: dict = {
        "project_id": project_id,
        "resources": []
    }

    for dataset in datasets:
        query_string = f"""
            SELECT
                table_name,
                table_type,
                COUNT(column_name) AS count_columns
            FROM
                `{project_id}.{dataset.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            LEFT JOIN (
                SELECT
                    *
                FROM
                    `{project_id}.{dataset.dataset_id}.INFORMATION_SCHEMA.TABLES`
            )
            USING
                (table_name)
            WHERE
                ENDS_WITH(column_name, '{search_string}') OR
                STARTS_WITH(column_name, '{search_string}')
            GROUP BY
                table_name,
                table_type
        """

        try:
            query_job = client.query(query_string).result()
            if query_job.total_rows <= 0:
                continue

            for row in query_job:
                column_app.get("resources").append({
                    "type": row.table_type,
                    "source": f"{dataset.dataset_id}.{row.table_name}",
                    "destination": None,
                    "number_of_appearances": row.count_columns
                })

        except BaseException as e:
            print(f"{project_id}: {e}")

    return column_app if len(column_app.get("resources")) > 0 else None

    # ------------- End of scanning table columns -------------


def search_queries(client, project_id, datasets, search_string: str):
    # ------------- Start to scan view's queries -------------

    query_app: dict = {
        "project_id": project_id,
        "resources": []
    }

    for dataset in datasets:
        tables = list(client.list_tables(f"{project_id}.{dataset.dataset_id}"))

        for table in tables:
            if table.table_type != "VIEW":
                continue

            view = client.get_table(f"{project_id}.{dataset.dataset_id}.{table.table_id}")
            if search_string in view.view_query:
                query_app.get("resources").append({
                    "type": "VIEW",
                    "source": f"{dataset.dataset_id}.{view.table_id}",
                    "destination": None,
                    "number_of_appearances": 1
                })

            else:
                continue

    return query_app if len(query_app.get("resources")) > 0 else None

    # ------------- End of scanning view's queries -------------


def search_scheduled_queries(project_id, search_string: str):

    # ------------- Start to scan scheduled-queries queries -------------

    try:
        scheduled_query_app: dict = {
            "project_id": project_id,
            "resources": []
        }

        client = bigquery_datatransfer.DataTransferServiceClient()
        parent = client.common_project_path(f"{project_id}/locations/EU")
        response_object = MessageToDict(client.list_transfer_configs(parent=parent)._pb)

        for response in response_object["transferConfigs"]:
            if response["dataSourceId"] != "scheduled_query":
                continue

            if search_string in response["params"]["query"]:  # (response["displayName"], f"{response.get('destinationDatasetId')}.{response['params'].get('destination_table_name_template')}")
                scheduled_query_app.get("resources").append({
                    "type": "SCHEDULED QUERY",
                    "source": response["displayName"],
                    "destination": f"{response.get('destinationDatasetId')}.{response['params'].get('destination_table_name_template')}",
                    "number_of_appearances": 1,
                })

            else:
                continue

        return scheduled_query_app if len(scheduled_query_app.get("resources")) > 0 else None

    except BaseException as e:
        print(f"{project_id}: {e}")

    # ------------- End of scanning scheduled-queries queries -------------


def search_scheduled_queries_with_table_name(project_id, search_string: str):

    # ------------- Start to scan scheduled-queries destination tables -------------

    try:
        scheduled_query_app: dict = {
            "project_id": project_id,
            "resources": []
        }

        client = bigquery_datatransfer.DataTransferServiceClient()
        parent = client.common_project_path(f"{project_id}/locations/EU")
        response_object = MessageToDict(client.list_transfer_configs(parent=parent)._pb)

        for response in response_object["transferConfigs"]:
            if response["dataSourceId"] != "scheduled_query" or "destination_table_name_template" not in response["params"]:
                continue

            if search_string in response["params"]["destination_table_name_template"]:
                scheduled_query_app.get("resources").append({
                    "type": "SCHEDULED QUERY",
                    "source": response["displayName"],
                    "destination": f"{response['destinationDatasetId']}.{response['params']['destination_table_name_template']}",
                    "number_of_appearances": 1
                })

            else:
                continue

        return scheduled_query_app if len(scheduled_query_app.get("resources")) > 0 else None

    except BaseException as e:
        print(f"{project_id}: {e}")

    # ------------- End of scanning scheduled-queries destination tables -------------


if __name__ == "__main__":
    main()

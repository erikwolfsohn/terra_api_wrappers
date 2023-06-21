import argparse
import terra_wrapper
from terra_wrapper import TerraWorkflows as tf

import importlib
importlib.reload(terra_wrapper)
from terra_wrapper import TerraWorkflows as tf


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Terra workflows from command line"
    )

    parser.add_argument(
        "-w", "--workspace", help="Workspace name", default="Workspace_Name"
    )
    parser.add_argument(
        "-n", "--namespace", help="Workspace namespace", default="Workspace_Namespace"
    )
    parser.add_argument(
        "-b", "--bucket", help="Workspace bucket", default="s3://Workspace_Bucket"
    )
    parser.add_argument("-t", "--table", help="Table name", default="Table_Name")

    return parser.parse_args()


def main():
    args = parse_args()

    ws_namespace = args.namespace
    ws_name = args.workspace
    ws_bucket = args.bucket
    table_name = args.table
    set_name = table_name + "_set"
    
    ws_vars = tf.updateWorkspaceVariables(ws_namespace, ws_name)
    
    run_id = tf.checkRunMailbox()

    run_data = tf.getRunContents(ws_namespace, ws_name, table_name, run_id)

    sub_results = tf.submitSampleWorkflow(
        ws_namespace, ws_name, ws_namespace, "TheiaCoV_FASTA", table_name, run_data[0]
    )
    tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

    run_data = tf.getRunContents(ws_namespace, ws_name, table_name, run_id)
    outrow = tf.resultToApollo(run_data[1]["results"])

    tf.createSampleSet(ws_namespace, ws_name, table_name, run_data[0], run_id[0])

    sub_results = tf.submitSampleWorkflow(
        ws_namespace, ws_name, ws_namespace, "NCBI_Scrub_SE", table_name, run_data[0]
    )
    tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

    sub_results = tf.submitSampleWorkflow(
        ws_namespace, ws_name, ws_namespace, "Mercury_SE_Prep", table_name, run_data[0]
    )
    tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

    sub_results = tf.submitSampleWorkflow(
        ws_namespace, ws_name, ws_namespace, "Mercury_Batch", set_name, run_id
    )
    tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

    sub_results = tf.submitSampleWorkflow(
        ws_namespace, ws_name, ws_namespace, "gisaid_cli3", set_name, run_id
    )


if __name__ == "__main__":
    main()

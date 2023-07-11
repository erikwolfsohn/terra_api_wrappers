from firecloud import fiss
import firecloud.api as fapi
import os
import io
import pandas as pd
import re
import json
#import collections
import sys
import csv
import logging
import time
from win32com.client import Dispatch
from requests.exceptions import ConnectionError as RequestsConnectionError

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

class TerraWorkflows:
    def __init__(self):
        pass

    def submitSampleWorkflow(
        ws_namespace, ws_name, cnamespace, wf_name, table_name, entity_list, MAX_RETRY=5
    ):
        i = 0
        retries = 0
        submission_results = []
        for i in range(len(entity_list)):
            stat_code = ""
            while stat_code != 201:
                try:
                    submission_results.insert(
                        i,
                        fapi.create_submission(
                            ws_namespace,
                            ws_name,
                            cnamespace,
                            wf_name,
                            etype=table_name,
                            entity=entity_list[i],
                        ),
                    )
                    stat_code = submission_results[i].status_code
                    time.sleep(retries * 5)
                except RequestsConnectionError as ex:
                    print("exception", ex, file=sys.stdout)
                    logging.exception(ex)
                    time.sleep(5)
                    continue
                finally:
                    print("status code: ", stat_code, file=sys.stdout)
                    if stat_code != 201:
                        retries += 1
                        if retries >= MAX_RETRY:
                            break

        return submission_results

    def waitForWorkflow(ws_namespace, ws_name, submission_results, MAX_RETRY=100):
        i = 0
        retries = 0
        for i in range(len(submission_results)):
            wf_stat_test = ""
            while wf_stat_test != "Done":
                try:
                    wf_sub_id = json.loads(submission_results[i].text)
                    wf_status = fapi.get_submission(
                        ws_namespace, ws_name, wf_sub_id["submissionId"]
                    )
                    s = json.loads(wf_status.text)
                    stat_code = wf_status.status_code
                    wf_stat_test = s["status"]
                    time.sleep(5)
                except RequestsConnectionError as ex:
                    print("exception", ex, file=sys.stdout)
                    logging.exception(ex)
                    time.sleep(5)
                    continue
                finally:
                    print(
                        "workflow status: ",
                        wf_stat_test,
                        " ",
                        stat_code,
                        file=sys.stdout,
                    )
                    if stat_code != 200:
                        retries += 1
                        time.sleep(15)
                        if retries >= MAX_RETRY:
                            break

    def getRunContents(ws_namespace, ws_name, table_name, run_id, MAX_RETRY=5):
        # MAX_RETRY = 5
        retries = 0
        stat_code = ""
        while stat_code != 200:
            try:
                run_contents = fapi.get_entities_query(
                    ws_namespace, ws_name, table_name, filter_terms=run_id
                )
                stat_code = run_contents.status_code
                time.sleep(retries * 5)
            except RequestsConnectionError as ex:
                print("exception", ex, file=sys.stdout)
                logging.exception(ex)
                time.sleep(5)
                continue
            finally:
                print("status code: ", stat_code, file=sys.stdout)
                if stat_code != 200:
                    retries += 1
                    if retries >= MAX_RETRY:
                        break

        run_text = json.loads(run_contents.text)
        i = 0
        rows = []
        for i in range(len(run_text["results"])):
            rows.append(run_text["results"][i]["name"])

        return rows, run_text

    def resultToApollo(run_metadata):
        outrow = {}
        for row in run_metadata:
            outrow[row["name"]] = {
                k: row["attributes"][k]
                for k in (
                    "specimen_accession_number",
                    "pango_lineage",
                    "clearlabs_assembly_coverage",
                )
            }

        for entry in outrow:
            if outrow[entry]["clearlabs_assembly_coverage"] <= 79.4:
                outrow[entry]["pango_lineage"] = "UNABLE TO BE SEQUENCED"

        df = pd.DataFrame(outrow)
        df = df.transpose()
        df.index.name = "SEQ ID"
        df.reset_index(inplace=True)
        df = df.drop("clearlabs_assembly_coverage", axis=1)
        # df.reset_index(drop=True, inplace=True)
        df.rename(
            columns={"specimen_accession_number": "entity:sample_id"}, inplace=True
        )
        df = df[["entity:sample_id", "pango_lineage", "SEQ ID"]]

        df.to_csv("WGS.csv", index=False)

    # return outrow

    def createSampleSet(ws_namespace, ws_name, table_name, run_data, run_id, MAX_RETRY=5):
        set_name = "membership:"+table_name+"_set_id"
        tsv_strings = pd.DataFrame(run_data)
        tsv_strings[set_name] = run_id
        tsv_strings.rename(columns={0: table_name}, inplace=True)
        tsv_strings = tsv_strings[[set_name, table_name]]
        tsv_strings = tsv_strings.to_csv(sep="\t", index=False)

        stat_code = ""
        retries = 0
        while stat_code != 200:
            try:
                tsv_up = fapi.upload_entities(ws_namespace, ws_name, tsv_strings)
                stat_code = tsv_up.status_code
            except RequestsConnectionError as ex:
                print("exception", ex, file=sys.stdout)
                logging.exception(ex)
                time.sleep(5)
                continue
            finally:
                print("status code: ", stat_code, file=sys.stdout)
                if stat_code != 200:
                    retries += 1
                    if retries >= MAX_RETRY:
                        break
                      
      
                      
    def updateWorkspaceVariables(ws_namespace, ws_name, MAX_RETRY=5):
      url = "https://theiagen.notion.site/Docker-Image-and-Reference-Materials-for-SARS-CoV-2-Genomic-Characterization-98328c61f5cb4f77975f512b55d09108"
      options = Options()
      options.add_argument("--headless=new")
      driver = webdriver.Chrome(
          service=Service(ChromeDriverManager().install()), options=options
      )
      driver.get(url)
      WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[@aria-label='Open']"))).click()
      notion_table = [
          my_elem.get_attribute("innerText")
          for my_elem in WebDriverWait(driver, 20).until(
              EC.visibility_of_all_elements_located(
                  (By.XPATH, "//div[@class='notion-table-cell-text']")
              )
          )
      ]
  
      workspace_keys = set(
          [
              "Pangolin Docker Image",
              "Nextclade Docker Image",
              "Nextclade Dataset Tag",
              "VADR Docker Image",
          ]
      )
      ccphl_keys = [
          "pangolin_docker_image",
          "nextclade_docker_image",
          "nextclade_dataset_tag",
          "vadr_docker_image",
      ]
      workspace_val_indices = [
          i for i, item in enumerate(notion_table) if item in workspace_keys
      ]
      workspace_val_indices = [i + 1 for i in workspace_val_indices]
      ws_update_vals = [notion_table[i] for i in workspace_val_indices]
      ws_update_vals = [val.strip('"') for val in ws_update_vals]
  
      ws_updates = []
      ws_attr = list(zip(ccphl_keys, ws_update_vals))
  
      for i in range(len(ws_update_vals)):
          print(ws_attr[i][0], ws_attr[i][1])
          ws_updates.append(fapi._attr_set(ws_attr[i][0], ws_attr[i][1]))
      
      stat_code = ""
      retries = 0
      while stat_code != 200:
          try:
              response = fapi.update_workspace_attributes(ws_namespace, ws_name, ws_updates)
              stat_code = response.status_code
          except RequestsConnectionError as ex:
              print("exception", ex, file=sys.stdout)
              logging.exception(ex)
              time.sleep(5)
              continue
          finally:
              print("status code: ", stat_code, file=sys.stdout)
              if stat_code != 200:
                  retries += 1
                  if retries >= MAX_RETRY:
                      break
      
      return ws_updates
                    
        
                
    def checkRunMailbox():
      outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
      root_folder = outlook.Folders.Item(1)
      print(root_folder.Name)
      for folder in root_folder.Folders:
          print(folder.Name)
  
      cl_notification_folder = root_folder.Folders["cl_instrument_notifications"]
      print(cl_notification_folder.Name)
  
      msg = cl_notification_folder.Items.GetFirst()
      msgbody = msg.body
      pattern = re.compile(
          r"BCGL[0-9]+\.[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]+", re.IGNORECASE
      )
      run_id = re.findall(pattern, msgbody)
      
      return run_id
    
    
    def abortWorkflow(ws_namespace, ws_name, submission_results, MAX_RETRY=100):
        i = 0
        retries = 0
        for i in range(len(submission_results)):
            wf_stat_test = ""
            while wf_stat_test != "Aborting":
                try:
                    wf_sub_id = json.loads(submission_results[i].text)
                    wf_status = fapi.abort_submission(
                        ws_namespace, ws_name, wf_sub_id["submissionId"]
                    )
                    wf_status = fapi.get_submission(
                        ws_namespace, ws_name, wf_sub_id["submissionId"]
                    )
                    s = json.loads(wf_status.text)
                    stat_code = wf_status.status_code
                    wf_stat_test = s["status"]
                    time.sleep(5)
                except RequestsConnectionError as ex:
                    print("exception", ex, file=sys.stdout)
                    logging.exception(ex)
                    time.sleep(5)
                    continue
                finally:
                    print(
                        "workflow status: ",
                        wf_stat_test,
                        " ",
                        stat_code,
                        file=sys.stdout,
                    )
                    if stat_code != 200:
                        retries += 1
                        time.sleep(15)
                        if retries >= MAX_RETRY:
                            break

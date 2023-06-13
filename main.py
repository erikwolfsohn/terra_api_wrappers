import terra_wrapper
import importlib
importlib.reload(terra_wrapper)
from terra_wrapper import TerraWorkflows as tf

from firecloud import fiss
#import firecloud
import firecloud.api as fapi
import os
import io
import pandas as pd
import re
import json
import collections
import subprocess, sys, os, re, argparse, textwrap
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

ws_namespace = 
ws_name = 
ws_bucket = 
table_name = 

url = 'https://theiagen.notion.site/Docker-Image-and-Reference-Materials-for-SARS-CoV-2-Genomic-Characterization-98328c61f5cb4f77975f512b55d09108'
options = Options()
options.add_argument("--headless=new")
driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
driver.get(url)
notion_table = [my_elem.get_attribute("innerText") for my_elem in WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located((By.XPATH, "//div[@class='notion-table-cell-text']")))]

workspace_keys = set(['Pangolin Docker Image','Nextclade Docker Image','Nextclade Dataset Tag','VADR Docker Image'])
ccphl_keys = ["pangolin_docker_image", "nextclade_docker_image", "nextclade_dataset_tag", "vadr_docker_image"]
workspace_val_indices = [i for i, item in enumerate(notion_table) if item in workspace_keys]
workspace_val_indices = [i+1 for i in workspace_val_indices]
ws_update_vals = [notion_table[i] for i in workspace_val_indices]
ws_update_vals = [val.strip('"') for val in ws_update_vals]

ws_updates = []
ws_attr = list(zip(ccphl_keys, ws_update_vals))

for i in range(len(ws_update_vals)):
  print(ws_attr[i][0],ws_attr[i][1])
  ws_updates.append(fapi._attr_set(ws_attr[i][0],ws_attr[i][1]))
  
response = fapi.update_workspace_attributes(ws_namespace, ws_name, ws_updates)

outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
root_folder = outlook.Folders.Item(1)
print (root_folder.Name)
for folder in root_folder.Folders:
    print (folder.Name)

cl_notification_folder=root_folder.Folders['cl_instrument_notifications']
print(cl_notification_folder.Name)

msg = cl_notification_folder.Items.GetFirst()
msgbody = msg.body
pattern = re.compile(r"BCGL[0-9]+\.[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]+", re.IGNORECASE)
run_id = re.findall(pattern,msgbody)

run_data = tf.getRunContents(ws_namespace, ws_name, table_name, run_id)

sub_results = tf.submitSampleWorkflow(ws_namespace, ws_name, ws_namespace, "TheiaCoV_FASTA", "sample", run_data[0])
tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

run_data = tf.getRunContents(ws_namespace, ws_name, table_name, run_id)
outrow = tf.resultToApollo(run_data[1]['results'])

tf.createSampleSet(ws_namespace, ws_name, run_data[0], run_id[0])

sub_results = tf.submitSampleWorkflow(ws_namespace, ws_name, ws_namespace, "NCBI_Scrub_SE", "sample", run_data[0])
tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

sub_results = tf.submitSampleWorkflow(ws_namespace, ws_name, ws_namespace, "Mercury_SE_Prep", "sample", run_data[0])
tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

sub_results = tf.submitSampleWorkflow(ws_namespace, ws_name, ws_namespace, "Mercury_Batch", "sample_set", run_id)
tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

sub_results = tf.submitSampleWorkflow(ws_namespace, ws_name, ws_namespace, "gisaid_cli3", "sample_set", run_id)
#tf.waitForWorkflow(ws_namespace, ws_name, sub_results)

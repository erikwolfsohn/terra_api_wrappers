class TerraWorkflows:
  def __init__(self):
    pass
  
  def submitSampleWorkflow(ws_namespace, ws_name, cnamespace, wf_name, table_name, entity_list, MAX_RETRY = 5):
    i = 0
    retries = 0
    submission_results = []
    for i in range(len(entity_list)):
      stat_code = ''
      while stat_code != 201:
        try: 
          submission_results.insert(i, fapi.create_submission(ws_namespace,ws_name,cnamespace, wf_name, etype=table_name, entity=entity_list[i]))
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
        
          
  def waitForWorkflow(ws_namespace, ws_name, submission_results, MAX_RETRY = 100):
    i = 0
    retries = 0
    for i in range(len(submission_results)):
      wf_stat_test = ''
      while wf_stat_test != 'Done':
        try:
          wf_sub_id = json.loads(submission_results[i].text)
          wf_status = fapi.get_submission(ws_namespace,ws_name,wf_sub_id['submissionId'])
          s = json.loads(wf_status.text)
          stat_code = wf_status.status_code
          wf_stat_test = s['status']
          time.sleep(5)
        except RequestsConnectionError as ex:
          print("exception", ex, file=sys.stdout)
          logging.exception(ex)
          time.sleep(5)
          continue
        finally:
          print("workflow status: ", wf_stat_test, " ", stat_code, file=sys.stdout)
          if stat_code != 200:
            retries += 1
            time.sleep(15)
            if retries >= MAX_RETRY:
              break
  
          
  def getRunContents(ws_namespace, ws_name, table_name, run_id, MAX_RETRY = 5):
    #MAX_RETRY = 5
    retries = 0
    stat_code = ''
    while stat_code != 200:
      try:
        run_contents = fapi.get_entities_query(ws_namespace, ws_name, table_name, filter_terms = run_id)
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
    for i in range(len(run_text['results'])):
      rows.append(run_text['results'][i]['name'])
    
    return rows, run_text
  
  
  def resultToApollo(run_metadata):
    outrow = {}
    for row in run_metadata:
      outrow[row['name']] = {k:row["attributes"][k] for k in ("specimen_accession_number", "pango_lineage", "clearlabs_assembly_coverage")}
      
    for entry in outrow:
      if outrow[entry]['clearlabs_assembly_coverage'] <= 79.4:
        outrow[entry]['pango_lineage'] = "UNABLE TO BE SEQUENCED"
        
    
    df = pd.DataFrame(outrow)
    df = df.transpose()
    df.index.name = "SEQ ID"
    df.reset_index(inplace=True)
    df = df.drop("clearlabs_assembly_coverage", axis=1)
    #df.reset_index(drop=True, inplace=True)
    df.rename(columns = {"specimen_accession_number":"entity:sample_id"}, inplace = True)  
    df = df[["entity:sample_id", "pango_lineage", "SEQ ID"]]
    
    df.to_csv("WGS.csv", index = False)
    #return outrow
    
    
  def createSampleSet(ws_namespace, ws_name, run_data, run_id, MAX_RETRY = 5):
    tsv_strings = pd.DataFrame(run_data)
    tsv_strings['membership:sample_set_id'] = run_id
    tsv_strings.rename(columns = {0:"sample"}, inplace = True)
    tsv_strings = tsv_strings[["membership:sample_set_id", "sample"]]
    tsv_strings = tsv_strings.to_csv(sep="\t", index=False)
    
    stat_code = ''
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
    #return tsv_up
            

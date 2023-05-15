# # Online Scoring using CML Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# Run this script to demonstrate online scoring using a CML Model.

# **Important:** Do the following before running this script:
# * Run `22_deploy_udf.py` to create `ir_model.pickle`
# * Deploy `ir_model.pickle` using the wrapper function in `23_deploy_cdsw.py` 
# * Update the `url` and `access_key` below


# ## Setup

import requests
import json


# ## Set the parameters for the model service

url = "https://modelservice.cdsw-gateway.dev.duocar.us/model"
access_key = "memlfywodj2yi2ks5850309v38nc96oa"
data = {"accessKey": access_key}
headers = {'Content-Type': 'application/json'}

# **Note:** These parameters are available in the **Overview** tab for the
# deployed model in CML.


# ## Get online predictions

for distance in [100, 1000, 10000, 100000, None]:
    
    # Update the request element.
    # This will be passed to the predict function specified in the model build.
    data["request"] = {"distance": distance}
  
    # Send a POST request to the CML Models API.
    # Convert the dictionary to a JSON string.
    r = requests.post(url, data=json.dumps(data), headers=headers)

    # Receive the JSON response and extract the predicted duration.
    duration = json.loads(r.content.decode())["response"]["duration"]

    # Print a message for the rider.
    if duration is not None:
      print(f"You will arrive at your destination in about {duration/60:.0f} minutes.")
    else:
      print("I have no idea when you will arrive at your destination!")

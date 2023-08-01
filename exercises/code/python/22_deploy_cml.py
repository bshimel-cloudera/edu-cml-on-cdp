# # Deploying a Machine Learning Model as a Rest API in CML

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# CML Native Workbench allows us to easily deploy our model
# and serve predictions via a REST API.  The first step in the process is to
# create a [wrapper function](https://en.wikipedia.org/wiki/Wrapper_function)
# around our model predict function that receives features in JSON format,
# applies the model, and returns the prediction in JSON format.  In this module
# we demonstrate how we can wrap our scikit-learn isotonic regression model for
# deployment in CML.  The [CML
# documentation](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-model-training-deployment.html)
# provides further details on actually deploying the model and obtaining
# predictions.

# **Important:** You must run `22_deploy_udf.py` before running this script.


# ## Setup

import pickle
import numpy as np


# ## Load the serialized model

with open("ir_model.pickle", "rb") as f:
  ir_model = pickle.load(f)


# ## Define a wrapper function to generate a prediction

def predict_cdsw(json_input):
  
  # Extract the features from the JSON input
  # (which looks like a Python dict object):
  distance = json_input["distance"]
  
  if distance is not None:
    
    # Reshape the features for the predict method:
    features = np.array([distance])
  
    # Compute the prediction:
    prediction = ir_model.predict(features)
  
    # Assemble the prediction into JSON object:
    json_output = {"duration": prediction[0]}
    
  else:
    json_output = {"duration": None}
  
  return json_output


# ## Test the function

predict_cdsw({"distance": 10000})
predict_cdsw({"distance": None})


# ## References

# [CML Documentation - Models](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-model-training-deployment.html)

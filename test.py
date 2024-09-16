import pandas as pd
import numpy as np

import time

import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.mixture import GaussianMixture
from sklearn.compose import ColumnTransformer
from sklearn import metrics
from tensorflow import keras
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import RidgeClassifierCV
from sklearn.feature_selection import SequentialFeatureSelector

from scipy.stats import f_oneway, chi2_contingency

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder, LabelEncoder, FunctionTransformer, StandardScaler

import pycaret.classification as pc

import boto3
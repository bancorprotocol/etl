# Databricks notebook source
"""
Instructions:

Updating the PoolCollection:
* Upload the new protocol jsons folder to '/dbfs/FileStore/tables/' as ETL_PROTOCOL_JSON_DIRECTORY
* In CMD 3: Update the `ETL_PROTOCOL_JSON_DIRECTORY` to this jsons folder 
* In CMD 11: Add a new contract dictionary for the new PoolCollectionTypeXVX using `load_contract`
* In CMD 11: Update the `PoolCollection` dictionary to the latest PoolCollection number
* In CMD 11: Add the new PoolCollectionTypeXVX dictionary to the `PoolCollectionSet`
* In CMD 12: Add Events as keys to the latest PoolCollectionTypeXVX dictionary
* In CMD 36 & 47: Add the new PoolCollectionTypeXVX address to the appropriate section (modification needed if new poolData events/structure)

Updating the Events:
* In CMD 12: Add Events as keys to the appropriate contract dictionary
* In CMD 13: Add the contract, event pair to the list



"""

# COMMAND ----------

from web3 import Web3
import pandas as pd
import datetime
import os
import json
import glob
from decimal import Decimal
import requests
import time
from sqlalchemy import create_engine
from bancor_etl.constants import *

# COMMAND ----------

url = f'https://eth-mainnet.alchemyapi.io/v2/{ETL_ALCHEMY_APIKEY}'

# COMMAND ----------

# HTTPProvider:
w3 = Web3(Web3.HTTPProvider(url))
w3.isConnected()

# COMMAND ----------

erc20_abi = '[{"inputs":[{"internalType":"uint256","name":"chainId_","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"guy","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":true,"inputs":[{"indexed":true,"internalType":"bytes4","name":"sig","type":"bytes4"},{"indexed":true,"internalType":"address","name":"usr","type":"address"},{"indexed":true,"internalType":"bytes32","name":"arg1","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg2","type":"bytes32"},{"indexed":false,"internalType":"bytes","name":"data","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"dst","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"burn","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"deny","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"holder","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"bool","name":"allowed","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"pull","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"push","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"rely","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"wards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'

v2poolToken_abi = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_disable","type":"bool"}],"name":"disableTransfers","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"standard","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_token","type":"address"},{"name":"_to","type":"address"},{"name":"_amount","type":"uint256"}],"name":"withdrawTokens","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"acceptOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_amount","type":"uint256"}],"name":"issue","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_amount","type":"uint256"}],"name":"destroy","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"transfersEnabled","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"newOwner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[{"name":"_name","type":"string"},{"name":"_symbol","type":"string"},{"name":"_decimals","type":"uint8"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_token","type":"address"}],"name":"NewSmartToken","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_amount","type":"uint256"}],"name":"Issuance","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_amount","type":"uint256"}],"name":"Destruction","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_spender","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_prevOwner","type":"address"},{"indexed":true,"name":"_newOwner","type":"address"}],"name":"OwnerUpdate","type":"event"}]'

converter_abi = '[{"inputs":[{"internalType":"contract IConverterAnchor","name":"_anchor","type":"address"},{"internalType":"contract IContractRegistry","name":"_registry","type":"address"},{"internalType":"uint32","name":"_maxConversionFee","type":"uint32"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint16","name":"_type","type":"uint16"},{"indexed":true,"internalType":"contract IConverterAnchor","name":"_anchor","type":"address"},{"indexed":true,"internalType":"bool","name":"_activated","type":"bool"}],"name":"Activation","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"contract IERC20Token","name":"_fromToken","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_toToken","type":"address"},{"indexed":true,"internalType":"address","name":"_trader","type":"address"},{"indexed":false,"internalType":"uint256","name":"_amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_return","type":"uint256"},{"indexed":false,"internalType":"int256","name":"_conversionFee","type":"int256"}],"name":"Conversion","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint32","name":"_prevFee","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"_newFee","type":"uint32"}],"name":"ConversionFeeUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"_provider","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"_amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newBalance","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newSupply","type":"uint256"}],"name":"LiquidityAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"_provider","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"_amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newBalance","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newSupply","type":"uint256"}],"name":"LiquidityRemoved","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"_prevOwner","type":"address"},{"indexed":true,"internalType":"address","name":"_newOwner","type":"address"}],"name":"OwnerUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"contract IERC20Token","name":"_token1","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_token2","type":"address"},{"indexed":false,"internalType":"uint256","name":"_rateN","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_rateD","type":"uint256"}],"name":"TokenRateUpdate","type":"event"},{"inputs":[],"name":"acceptAnchorOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"acceptOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"acceptTokenOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_reserve1Amount","type":"uint256"},{"internalType":"uint256","name":"_reserve2Amount","type":"uint256"},{"internalType":"uint256","name":"_minReturn","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"},{"internalType":"uint256[]","name":"_reserveAmounts","type":"uint256[]"},{"internalType":"uint256","name":"_minReturn","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMu|tability":"payable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"},{"internalType":"uint256","name":"_reserveTokenIndex","type":"uint256"},{"internalType":"uint256","name":"_reserveAmount","type":"uint256"}],"name":"addLiquidityCost","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"},{"internalType":"uint256","name":"_reserveAmount","type":"uint256"}],"name":"addLiquidityReturn","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_token","type":"address"},{"internalType":"uint32","name":"_weight","type":"uint32"}],"name":"addReserve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"anchor","outputs":[{"internalType":"contract IConverterAnchor","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"averageRateInfo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"connectorTokenCount","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_index","type":"uint256"}],"name":"connectorTokens","outputs":[{"internalType":"contract IERC20Token","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_address","type":"address"}],"name":"connectors","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint32","name":"","type":"uint32"},{"internalType":"bool","name":"","type":"bool"},{"internalType":"bool","name":"","type":"bool"},{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"conversionFee","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_sourceToken","type":"address"},{"internalType":"contract IERC20Token","name":"_targetToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"address","name":"_trader","type":"address"},{"internalType":"address payable","name":"_beneficiary","type":"address"}],"name":"convert","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"converterType","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_connectorToken","type":"address"}],"name":"getConnectorBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_sourceToken","type":"address"},{"internalType":"contract IERC20Token","name":"_targetToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"getReturn","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isV28OrHigher","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"maxConversionFee","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"newOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"onlyOwnerCanUpdateRegistry","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"prevRegistry","outputs":[{"internalType":"contract IContractRegistry","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_token","type":"address"}],"name":"recentAverageRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"registry","outputs":[{"internalType":"contract IContractRegistry","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_reserve1MinReturn","type":"uint256"},{"internalType":"uint256","name":"_reserve2MinReturn","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"},{"internalType":"uint256[]","name":"_reserveMinReturnAmounts","type":"uint256[]"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"}],"name":"removeLiquidityReturn","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"}],"name":"reserveBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reserveBalances","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reserveTokenCount","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reserveTokens","outputs":[{"internalType":"contract IERC20Token[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"}],"name":"reserveWeight","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"restoreRegistry","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_onlyOwnerCanUpdateRegistry","type":"bool"}],"name":"restrictRegistryUpdate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint32","name":"_conversionFee","type":"uint32"}],"name":"setConversionFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_sourceToken","type":"address"},{"internalType":"contract IERC20Token","name":"_targetToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"targetAmountAndFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token","outputs":[{"internalType":"contract IConverterAnchor","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_newOwner","type":"address"}],"name":"transferAnchorOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newOwner","type":"address"}],"name":"transferTokenOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"updateRegistry","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"upgrade","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address payable","name":"_to","type":"address"}],"name":"withdrawETH","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_token","type":"address"},{"internalType":"address","name":"_to","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"withdrawTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]'

# COMMAND ----------

def get_spot_rate(bntTradingLiquidity, baseTokenTradingLiquidity, baseTokendecimals):
    bntTradingLiquidity_real = Decimal(bntTradingLiquidity) / Decimal('10')**Decimal('18')
    baseTokenTradingLiquidity_real = Decimal(baseTokenTradingLiquidity) / Decimal('10')**Decimal(baseTokendecimals)
    
    if baseTokenTradingLiquidity == 0:
        return(0)
    else:
        return(bntTradingLiquidity_real/baseTokenTradingLiquidity_real)
    
def get_emaDeviation(emaRate, spotRate):
    if emaRate == 0:
        return(0)
    else:
        return(round(abs((emaRate - spotRate)/emaRate),10))
    
def get_emaInvDeviation(emaInvRate, spotRate):
    if emaInvRate == 0:
        return(0)
    else:
        emaRate = Decimal('1')/emaInvRate
        return(round(abs((emaRate - spotRate)/emaRate),10))
    
def get_surplus_percent(masterVaultTknBalance_real, stakedBalance_real):
    if stakedBalance_real == 0:
        return(0)
    else:
        return(round((masterVaultTknBalance_real - stakedBalance_real)/stakedBalance_real,6))
      
def get_safe_divide(numerator, denominator):
    if denominator == 0:
        return(0)
    else:
        return(numerator/denominator)

# COMMAND ----------

def get_token_contract(tknAddress):
    if tknAddress == '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE':
        return(None)
    else:
        return(w3.eth.contract(address=tknAddress, abi=erc20_abi))

def get_token_symbols(tknContracts):
    symbols = {}
    for key in tknContracts.keys():
        if key == '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE':
            symbols['eth']=w3.toChecksumAddress(key)
        elif key == '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2':
            symbols['mkr']=w3.toChecksumAddress(key)  
        else:
            symbols[tknContracts[key].functions.symbol().call().lower()] = w3.toChecksumAddress(key)
    return(symbols)
        
def get_pool_tokens(tokenAddresses):
    poolTokenAddresses = {}
    for key in tokenAddresses.keys():
        if tokenAddresses[key] == '0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C':
            poolTokenAddresses['bnbnt'] = '0xAB05Cf7C6c3a288cd36326e4f7b8600e7268E344'
        else:
            poolTokenAddresses[f"bn{key}"] = PoolCollection['contract'].functions.poolToken(tokenAddresses[key]).call()
    return(poolTokenAddresses)

def get_pool_token_contracts(poolTokenAddresses):
    BNTPoolToken = load_contract('bnBNT')
    return({x[0]:w3.eth.contract(address=x[1], abi=BNTPoolToken['abi']) for x in poolTokenAddresses.items()})

def get_pool_token_supply(poolTokenContracts):
    return({x[0]:x[1].functions.totalSupply().call() for x in poolTokenContracts.items()})

def get_pool_token_balance(poolTokenContracts, wallet_address):
    return({x[0]:x[1].functions.balanceOf(wallet_address).call() for x in poolTokenContracts.items()})

# COMMAND ----------

def update_tokenInfo():
    tknAddresses = NetworkSettings['contract'].functions.protectedTokenWhitelist().call()
    tokenInfo = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'tokenInfo.parquet')

    missingAddresses = list(set(tknAddresses) - set(tokenInfo.tokenAddress)) 
    missingContracts = [get_token_contract(x) for x in missingAddresses]
    missingSymbols = [x.functions.symbol().call().lower() for x in missingContracts]
    missingDecimals = [str(x.functions.decimals().call()) for x in missingContracts]
    missingtokenAddresses = {missingSymbols[i]:missingAddresses[i] for i in range(len(missingAddresses))}
    missingpoolTokenAddresses = get_pool_tokens(missingtokenAddresses)
    missingdf = pd.DataFrame([missingAddresses, missingSymbols, missingDecimals, list(missingpoolTokenAddresses.values())], index = ['tokenAddress','symbol','decimals', 'poolTokenAddress']).T

    tokenInfo2 = tokenInfo.append(missingdf)
    tokenInfo2.sort_values(by='symbol', inplace=True)
    tokenInfo2.reset_index(inplace=True, drop=True)

    #check again for missing poolTokenAddreses as tokens are not allocated until they become pools
    emptys = tokenInfo2[tokenInfo2.poolTokenAddress=='0x0000000000000000000000000000000000000000'].copy()
    emptytokenAddresses = {emptys.symbol[i]: emptys.tokenAddress[i] for i in emptys.index}
    emptypoolTokenAddresses = get_pool_tokens(emptytokenAddresses)
    emptys.loc[:,'poolTokenAddress'] = list(emptypoolTokenAddresses.values())

    tokenInfo3 = tokenInfo2.drop(emptys.index).append(emptys)
    tokenInfo3.sort_values(by='symbol', inplace=True)
    tokenInfo3.reset_index(inplace=True, drop=True)
    tokenInfo3 = tokenInfo3.astype(str)
    tokenInfo3.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'tokenInfo.parquet', compression='gzip')

    return(tokenInfo3)

# COMMAND ----------

def update_blockNumber_to_timestamp():
    currentBlock = w3.eth.get_block('latest')
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    blocks = list(range(blockNumber_to_timestamp.blockNumber.max(),currentBlock['number']))
    timestamps = [int(w3.eth.getBlock(x).timestamp) for x in blocks]
    times = [datetime.datetime.fromtimestamp(x).astimezone(datetime.timezone.utc) for x in timestamps]
    df = blockNumber_to_timestamp.append(pd.DataFrame([blocks,timestamps,times], index=['blockNumber','timestamp','time']).T)
    df = df[~df.timestamp.duplicated()].copy()
    df.sort_values(by='timestamp', inplace=True)
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet', compression='gzip', allow_truncated_timestamps=True)

# COMMAND ----------

def update_maxpositions():
    maxpositionsdf = pd.read_csv(ETL_CSV_STORAGE_DIRECTORY+"positions.csv", dtype=str)
    maxpositionsdf.fillna(method='ffill', inplace=True)  ## note this messes up the blocknumber
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    blockNumber_to_timestamp['day'] = [str(blockNumber_to_timestamp.time[i])[:10] for i in blockNumber_to_timestamp.index]
    btts = pd.merge(blockNumber_to_timestamp, maxpositionsdf[['day', 'max_position']], how='left', on='day')
    btts.fillna(method='ffill', inplace=True)
    btts = btts[['day', 'blockNumber', 'max_position']].copy()
    btts.rename(columns = {'blockNumber':'block_number'}, inplace=True)
    btts = btts.groupby('day').last().reset_index()[:-1]
    daylist = list(btts.day)[:-1]
    maxpositionsdf = maxpositionsdf[~maxpositionsdf.day.isin(daylist)].append(btts)
    maxpositionsdf.rename(columns = {'block_number':'blocknumber'}, inplace=True)
    maxpositionsdf.reset_index(inplace=True, drop=True)
    maxpositionsdf = maxpositionsdf.astype(str)
    maxpositionsdf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'maxpositionsdf.parquet', compression='gzip')

# COMMAND ----------

def load_contract(base_contract_string):
    proxy_file = ETL_PROTOCOL_JSON_DIRECTORY+"/"+base_contract_string+"_Proxy.json"
    implementation_file = ETL_PROTOCOL_JSON_DIRECTORY+"/"+base_contract_string+"_Implementation.json"
    standard_file = ETL_PROTOCOL_JSON_DIRECTORY+"/"+base_contract_string+".json"
    
    if os.path.exists(proxy_file): 
        g = open(proxy_file)
        proxy_contract_data = json.load(g)
        addy = w3.toChecksumAddress(proxy_contract_data['address'])
        g.close()
    else:
        f = open(standard_file)
        standard_contract_data = json.load(f)
        addy = w3.toChecksumAddress(standard_contract_data['address'])
        f.close() 
        
    if os.path.exists(implementation_file):
        f = open(implementation_file)
        impl_contract_data = json.load(f)
        abi = impl_contract_data['abi']
        f.close()
    else:
        f = open(standard_file)
        standard_contract_data = json.load(f)
        abi = standard_contract_data['abi']
        f.close() 
        
    contract = w3.eth.contract(address=addy, abi=abi)
    return({'name': base_contract_string,'addy':addy, 'abi':abi, 'contract':contract})

# COMMAND ----------

BNT = load_contract('BNT')
BNTGovernance = load_contract('BNTGovernance')
BNTPool = load_contract('BNTPool')
BNTPoolProxy = load_contract('BNTPoolProxy')
BancorNetwork = load_contract('BancorNetwork')
BancorNetworkInfo = load_contract('BancorNetworkInfo')
BancorNetworkProxy = load_contract('BancorNetworkProxy')
BancorPortal = load_contract('BancorPortal')
BancorV1Migration = load_contract('BancorV1Migration')
ExternalProtectionVault = load_contract('ExternalProtectionVault')
ExternalRewardsVault = load_contract('ExternalRewardsVault')
# LiquidityProtection = load_contract('LiquidityProtection')
MasterVault = load_contract('MasterVault')
NetworkSettings = load_contract('NetworkSettings')
NetworkSettings_old = load_contract('NetworkSettings_old')
PendingWithdrawals = load_contract('PendingWithdrawals')

PoolCollectionType1V1 = load_contract('PoolCollectionType1V1')
PoolCollectionType1V2 = load_contract('PoolCollectionType1V2')
PoolCollectionType1V3 = load_contract('PoolCollectionType1V3')
PoolCollectionType1V4 = load_contract('PoolCollectionType1V4')
PoolCollectionType1V5 = load_contract('PoolCollectionType1V5')
PoolCollectionType1V6 = load_contract('PoolCollectionType1V6')
PoolCollectionType1V7 = load_contract('PoolCollectionType1V7')
PoolCollectionType1V8 = load_contract('PoolCollectionType1V8')
PoolCollectionType1V9 = load_contract('PoolCollectionType1V9')
PoolCollectionType1V10 = load_contract('PoolCollectionType1V10')
PoolCollection = load_contract('PoolCollectionType1V10')  ## specific for most recent
# make sure to update the events below!!!!!!!!!!!!!!
PoolCollectionset = [PoolCollectionType1V1, PoolCollectionType1V2, PoolCollectionType1V3, PoolCollectionType1V4, PoolCollectionType1V5, PoolCollectionType1V6, PoolCollectionType1V7, PoolCollectionType1V8, PoolCollectionType1V9, PoolCollectionType1V10]
PoolCollectionAddys = {PoolCollectionset[i]['addy']:i for i in range(len(PoolCollectionset))}

PoolMigrator = load_contract('PoolMigrator')
PoolTokenFactory = load_contract('PoolTokenFactory')
ProxyAdmin = load_contract('ProxyAdmin')
StandardRewards = load_contract('StandardRewards')
StakingRewardsClaim = load_contract('StakingRewardsClaim')
VBNT = load_contract('VBNT')
VBNTGovernance = load_contract('VBNTGovernance')
bnBNT = load_contract('bnBNT')

# COMMAND ----------

# Load EVENTS
BancorNetwork['FlashLoanCompleted'] = BancorNetwork['contract'].events.FlashLoanCompleted()
BancorNetwork['FundsMigrated'] = BancorNetwork['contract'].events.FundsMigrated()
BancorNetwork['NetworkFeesWithdrawn'] = BancorNetwork['contract'].events.NetworkFeesWithdrawn()
BancorNetwork['PoolAdded'] = BancorNetwork['contract'].events.PoolAdded()
BancorNetwork['PoolCollectionAdded'] = BancorNetwork['contract'].events.PoolCollectionAdded()
BancorNetwork['PoolCollectionRemoved'] = BancorNetwork['contract'].events.PoolCollectionRemoved()
BancorNetwork['PoolCreated'] = BancorNetwork['contract'].events.PoolCreated()
BancorNetwork['PoolRemoved'] = BancorNetwork['contract'].events.PoolRemoved()
BancorNetwork['TokensTraded'] = BancorNetwork['contract'].events.TokensTraded()

BancorPortal['SushiSwapPositionMigrated'] = BancorPortal['contract'].events.SushiSwapPositionMigrated()
BancorPortal['UniswapV2PositionMigrated'] = BancorPortal['contract'].events.UniswapV2PositionMigrated()

BancorV1Migration['PositionMigrated'] = BancorV1Migration['contract'].events.PositionMigrated()

BNTPool['FundingRenounced'] = BNTPool['contract'].events.FundingRenounced()
BNTPool['FundingRequested'] = BNTPool['contract'].events.FundingRequested()
BNTPool['FundsBurned'] = BNTPool['contract'].events.FundsBurned()
BNTPool['FundsWithdrawn'] = BNTPool['contract'].events.FundsWithdrawn()
BNTPool['TokensDeposited'] = BNTPool['contract'].events.TokensDeposited()
BNTPool['TokensWithdrawn'] = BNTPool['contract'].events.TokensWithdrawn()
BNTPool['TotalLiquidityUpdated'] = BNTPool['contract'].events.TotalLiquidityUpdated()

ExternalProtectionVault['FundsBurned'] = ExternalProtectionVault['contract'].events.FundsBurned()
ExternalProtectionVault['FundsWithdrawn'] = ExternalProtectionVault['contract'].events.FundsWithdrawn()

ExternalRewardsVault['FundsBurned'] = ExternalRewardsVault['contract'].events.FundsBurned()
ExternalRewardsVault['FundsWithdrawn'] = ExternalRewardsVault['contract'].events.FundsWithdrawn()

MasterVault['FundsBurned'] = MasterVault['contract'].events.FundsBurned()
MasterVault['FundsWithdrawn'] = MasterVault['contract'].events.FundsWithdrawn()

NetworkSettings['DefaultFlashLoanFeePPMUpdated'] = NetworkSettings['contract'].events.DefaultFlashLoanFeePPMUpdated()
NetworkSettings['FlashLoanFeePPMUpdated'] = NetworkSettings['contract'].events.FlashLoanFeePPMUpdated()
NetworkSettings['FundingLimitUpdated'] = NetworkSettings['contract'].events.FundingLimitUpdated()
NetworkSettings['MinLiquidityForTradingUpdated'] = NetworkSettings['contract'].events.MinLiquidityForTradingUpdated()
NetworkSettings['TokenAddedToWhitelist'] = NetworkSettings['contract'].events.TokenAddedToWhitelist()
NetworkSettings['TokenRemovedFromWhitelist'] = NetworkSettings['contract'].events.TokenRemovedFromWhitelist()
NetworkSettings['VortexBurnRewardUpdated'] = NetworkSettings['contract'].events.VortexBurnRewardUpdated()
NetworkSettings['WithdrawalFeePPMUpdated'] = NetworkSettings['contract'].events.WithdrawalFeePPMUpdated()

PendingWithdrawals['WithdrawalCancelled'] = PendingWithdrawals['contract'].events.WithdrawalCancelled()
PendingWithdrawals['WithdrawalCompleted'] = PendingWithdrawals['contract'].events.WithdrawalCompleted()
PendingWithdrawals['WithdrawalInitiated'] = PendingWithdrawals['contract'].events.WithdrawalInitiated()

StakingRewardsClaim['RewardsClaimed'] = StakingRewardsClaim['contract'].events.RewardsClaimed()
StakingRewardsClaim['RewardsStaked'] = StakingRewardsClaim['contract'].events.RewardsStaked()

StandardRewards['ProgramCreated'] = StandardRewards['contract'].events.ProgramCreated()
StandardRewards['ProgramEnabled'] = StandardRewards['contract'].events.ProgramEnabled()
StandardRewards['ProgramTerminated'] = StandardRewards['contract'].events.ProgramTerminated()
StandardRewards['ProviderJoined'] = StandardRewards['contract'].events.ProviderJoined()
StandardRewards['ProviderLeft'] = StandardRewards['contract'].events.ProviderLeft()
StandardRewards['RewardsClaimed'] = StandardRewards['contract'].events.RewardsClaimed()
StandardRewards['RewardsStaked'] = StandardRewards['contract'].events.RewardsStaked()

BNT['Issuance'] = BNT['contract'].events.Issuance()
BNT['Destruction'] = BNT['contract'].events.Destruction()
VBNT['Issuance'] = VBNT['contract'].events.Issuance()
VBNT['Destruction'] = VBNT['contract'].events.Destruction()

PoolCollectionType1V1['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V1['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V1['DepositingEnabled'] = PoolCollectionType1V1['contract'].events.DepositingEnabled()
PoolCollectionType1V1['TokensDeposited'] = PoolCollectionType1V1['contract'].events.TokensDeposited()
PoolCollectionType1V1['TokensWithdrawn'] = PoolCollectionType1V1['contract'].events.TokensWithdrawn()
PoolCollectionType1V1['TotalLiquidityUpdated'] = PoolCollectionType1V1['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V1['TradingEnabled'] = PoolCollectionType1V1['contract'].events.TradingEnabled()
PoolCollectionType1V1['TradingFeePPMUpdated'] = PoolCollectionType1V1['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V1['TradingLiquidityUpdated'] = PoolCollectionType1V1['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V2['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V2['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V2['DepositingEnabled'] = PoolCollectionType1V2['contract'].events.DepositingEnabled()
PoolCollectionType1V2['TokensDeposited'] = PoolCollectionType1V2['contract'].events.TokensDeposited()
PoolCollectionType1V2['TokensWithdrawn'] = PoolCollectionType1V2['contract'].events.TokensWithdrawn()
PoolCollectionType1V2['TotalLiquidityUpdated'] = PoolCollectionType1V2['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V2['TradingEnabled'] = PoolCollectionType1V2['contract'].events.TradingEnabled()
PoolCollectionType1V2['TradingFeePPMUpdated'] = PoolCollectionType1V2['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V2['TradingLiquidityUpdated'] = PoolCollectionType1V2['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V3['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V3['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V3['DepositingEnabled'] = PoolCollectionType1V3['contract'].events.DepositingEnabled()
PoolCollectionType1V3['TokensDeposited'] = PoolCollectionType1V3['contract'].events.TokensDeposited()
PoolCollectionType1V3['TokensWithdrawn'] = PoolCollectionType1V3['contract'].events.TokensWithdrawn()
PoolCollectionType1V3['TotalLiquidityUpdated'] = PoolCollectionType1V3['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V3['TradingEnabled'] = PoolCollectionType1V3['contract'].events.TradingEnabled()
PoolCollectionType1V3['TradingFeePPMUpdated'] = PoolCollectionType1V3['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V3['TradingLiquidityUpdated'] = PoolCollectionType1V3['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V4['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V4['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V4['DepositingEnabled'] = PoolCollectionType1V4['contract'].events.DepositingEnabled()
PoolCollectionType1V4['TokensDeposited'] = PoolCollectionType1V4['contract'].events.TokensDeposited()
PoolCollectionType1V4['TokensWithdrawn'] = PoolCollectionType1V4['contract'].events.TokensWithdrawn()
PoolCollectionType1V4['TotalLiquidityUpdated'] = PoolCollectionType1V4['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V4['TradingEnabled'] = PoolCollectionType1V4['contract'].events.TradingEnabled()
PoolCollectionType1V4['TradingFeePPMUpdated'] = PoolCollectionType1V4['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V4['TradingLiquidityUpdated'] = PoolCollectionType1V4['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V5['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V5['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V5['DepositingEnabled'] = PoolCollectionType1V5['contract'].events.DepositingEnabled()
PoolCollectionType1V5['TokensDeposited'] = PoolCollectionType1V5['contract'].events.TokensDeposited()
PoolCollectionType1V5['TokensWithdrawn'] = PoolCollectionType1V5['contract'].events.TokensWithdrawn()
PoolCollectionType1V5['TotalLiquidityUpdated'] = PoolCollectionType1V5['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V5['TradingEnabled'] = PoolCollectionType1V5['contract'].events.TradingEnabled()
PoolCollectionType1V5['TradingFeePPMUpdated'] = PoolCollectionType1V5['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V5['TradingLiquidityUpdated'] = PoolCollectionType1V5['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V6['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V6['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V6['DepositingEnabled'] = PoolCollectionType1V6['contract'].events.DepositingEnabled()
PoolCollectionType1V6['TokensDeposited'] = PoolCollectionType1V6['contract'].events.TokensDeposited()
PoolCollectionType1V6['TokensWithdrawn'] = PoolCollectionType1V6['contract'].events.TokensWithdrawn()
PoolCollectionType1V6['TotalLiquidityUpdated'] = PoolCollectionType1V6['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V6['TradingEnabled'] = PoolCollectionType1V6['contract'].events.TradingEnabled()
PoolCollectionType1V6['TradingFeePPMUpdated'] = PoolCollectionType1V6['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V6['TradingLiquidityUpdated'] = PoolCollectionType1V6['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V7['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V7['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V7['DepositingEnabled'] = PoolCollectionType1V7['contract'].events.DepositingEnabled()
PoolCollectionType1V7['TokensDeposited'] = PoolCollectionType1V7['contract'].events.TokensDeposited()
PoolCollectionType1V7['TokensWithdrawn'] = PoolCollectionType1V7['contract'].events.TokensWithdrawn()
PoolCollectionType1V7['TotalLiquidityUpdated'] = PoolCollectionType1V7['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V7['TradingEnabled'] = PoolCollectionType1V7['contract'].events.TradingEnabled()
PoolCollectionType1V7['TradingFeePPMUpdated'] = PoolCollectionType1V7['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V7['TradingLiquidityUpdated'] = PoolCollectionType1V7['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V8['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V8['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V8['DepositingEnabled'] = PoolCollectionType1V8['contract'].events.DepositingEnabled()
PoolCollectionType1V8['TokensDeposited'] = PoolCollectionType1V8['contract'].events.TokensDeposited()
PoolCollectionType1V8['TokensWithdrawn'] = PoolCollectionType1V8['contract'].events.TokensWithdrawn()
PoolCollectionType1V8['TotalLiquidityUpdated'] = PoolCollectionType1V8['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V8['TradingEnabled'] = PoolCollectionType1V8['contract'].events.TradingEnabled()
PoolCollectionType1V8['TradingFeePPMUpdated'] = PoolCollectionType1V8['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V8['TradingLiquidityUpdated'] = PoolCollectionType1V8['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V9['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V9['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V9['DepositingEnabled'] = PoolCollectionType1V9['contract'].events.DepositingEnabled()
PoolCollectionType1V9['TokensDeposited'] = PoolCollectionType1V9['contract'].events.TokensDeposited()
PoolCollectionType1V9['TokensWithdrawn'] = PoolCollectionType1V9['contract'].events.TokensWithdrawn()
PoolCollectionType1V9['TotalLiquidityUpdated'] = PoolCollectionType1V9['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V9['TradingEnabled'] = PoolCollectionType1V9['contract'].events.TradingEnabled()
PoolCollectionType1V9['TradingFeePPMUpdated'] = PoolCollectionType1V9['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V9['TradingLiquidityUpdated'] = PoolCollectionType1V9['contract'].events.TradingLiquidityUpdated()

PoolCollectionType1V10['DefaultTradingFeePPMUpdated'] = PoolCollectionType1V10['contract'].events.DefaultTradingFeePPMUpdated()
PoolCollectionType1V10['DepositingEnabled'] = PoolCollectionType1V10['contract'].events.DepositingEnabled()
PoolCollectionType1V10['TokensDeposited'] = PoolCollectionType1V10['contract'].events.TokensDeposited()
PoolCollectionType1V10['TokensWithdrawn'] = PoolCollectionType1V10['contract'].events.TokensWithdrawn()
PoolCollectionType1V10['TotalLiquidityUpdated'] = PoolCollectionType1V10['contract'].events.TotalLiquidityUpdated()
PoolCollectionType1V10['TradingEnabled'] = PoolCollectionType1V10['contract'].events.TradingEnabled()
PoolCollectionType1V10['TradingFeePPMUpdated'] = PoolCollectionType1V10['contract'].events.TradingFeePPMUpdated()
PoolCollectionType1V10['TradingLiquidityUpdated'] = PoolCollectionType1V10['contract'].events.TradingLiquidityUpdated()


# COMMAND ----------

tokenInfo = update_tokenInfo()
tokenAddresses = {tokenInfo.symbol[i]:tokenInfo.tokenAddress[i] for i in tokenInfo.index}
tokenSymbols = {tokenInfo.tokenAddress[i]:tokenInfo.symbol[i] for i in tokenInfo.index}
tokenDecimals = {tokenInfo.symbol[i]:str(tokenInfo.decimals[i]) for i in tokenInfo.index}
tokenContracts = {tokenInfo.symbol[i]:get_token_contract(tokenInfo.tokenAddress[i]) for i in tokenInfo.index}
poolTokenAddresses = {'bn'+str(tokenInfo.symbol[i]):tokenInfo.poolTokenAddress[i] for i in tokenInfo.index}
poolTokenContracts = get_pool_token_contracts(poolTokenAddresses)
tokenDecimals2 = tokenDecimals.copy()
for tkn in tokenDecimals2.keys():
    tokenDecimals[f'bn{tkn}'] = tokenDecimals[tkn]

# COMMAND ----------

update_blockNumber_to_timestamp()

# COMMAND ----------

update_maxpositions()

# COMMAND ----------

events_list = [(BancorNetwork,"FlashLoanCompleted"),
                (BancorNetwork,"FundsMigrated"),
                (BancorNetwork,"NetworkFeesWithdrawn"),
                (BancorNetwork,"PoolAdded"),
                (BancorNetwork,"PoolCollectionAdded"),
                (BancorNetwork,"PoolCollectionRemoved"),
                (BancorNetwork,"PoolCreated"),
                (BancorNetwork,"PoolRemoved"),
                (BancorNetwork,"TokensTraded"),

                (BancorPortal,"SushiSwapPositionMigrated"),
                (BancorPortal,"UniswapV2PositionMigrated"),

                (BancorV1Migration,"PositionMigrated"),

                (BNTPool,"FundingRenounced"),
                (BNTPool,"FundingRequested"),
                (BNTPool,"FundsBurned"),
                (BNTPool,"FundsWithdrawn"),
                (BNTPool,"TokensDeposited"),
                (BNTPool,"TokensWithdrawn"),
                (BNTPool,"TotalLiquidityUpdated"),

                (ExternalProtectionVault,"FundsBurned"),
                (ExternalProtectionVault,"FundsWithdrawn"),

                (ExternalRewardsVault,"FundsBurned"),
                (ExternalRewardsVault,"FundsWithdrawn"),

                (MasterVault,"FundsBurned"),
                (MasterVault,"FundsWithdrawn"),
              
                (NetworkSettings,"DefaultFlashLoanFeePPMUpdated"),
                (NetworkSettings,"FlashLoanFeePPMUpdated"),
                (NetworkSettings,"FundingLimitUpdated"),
                (NetworkSettings,"MinLiquidityForTradingUpdated"),
                (NetworkSettings,"TokenAddedToWhitelist"),
                (NetworkSettings,"TokenRemovedFromWhitelist"),
                (NetworkSettings,"VortexBurnRewardUpdated"),
                (NetworkSettings,"WithdrawalFeePPMUpdated"),
               
                (PendingWithdrawals,"WithdrawalCancelled"),
                (PendingWithdrawals,"WithdrawalCompleted"),
                (PendingWithdrawals,"WithdrawalInitiated"),

                (StakingRewardsClaim,"RewardsClaimed"),
                (StakingRewardsClaim,"RewardsStaked"),

                (StandardRewards,"ProgramCreated"),
                (StandardRewards,"ProgramEnabled"),
                (StandardRewards,"ProgramTerminated"),
                (StandardRewards,"ProviderJoined"),
                (StandardRewards,"ProviderLeft"),
                (StandardRewards,"RewardsClaimed"),
                (StandardRewards,"RewardsStaked"),
                
                (BNT,"Issuance"),
                (BNT,"Destruction"),
                (VBNT,"Issuance"),
                (VBNT,"Destruction")
               
              ]

# COMMAND ----------

fromBlock = 14609000

# COMMAND ----------

def update_PoolCollection_events():
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    toBlock = int(blockNumber_to_timestamp.blockNumber.max())

    for name_of_event in ["DefaultTradingFeePPMUpdated", "DepositingEnabled", "TokensDeposited", "TokensWithdrawn",
                         "TotalLiquidityUpdated", "TradingEnabled", "TradingFeePPMUpdated", "TradingLiquidityUpdated"]:    
    
        file_location = ETL_CSV_STORAGE_DIRECTORY+f'Events_PoolCollection_{str(name_of_event)}.parquet'
        file_location_par = ETL_CSV_STORAGE_DIRECTORY+f'Events_PoolCollection_{str(name_of_event)}.parquet'
        if os.path.isfile(file_location):
            master = pd.read_parquet(file_location)
            if len(master) != 0:
                master.loc[:,'blocknumber'] = [int(float(x)) for x in master.blocknumber]
                master.loc[:,'timestamp'] = [int(float(x)) for x in master.timestamp]
                fromBlock = int(master.blocknumber.max())
            else:
                fromBlock = 14609000
                master = pd.DataFrame()
        else:
            fromBlock = 14609000
            master = pd.DataFrame()
            
        collectedData = pd.DataFrame()
        for PoolCollection1 in PoolCollectionset:
            try:
                event_filter = PoolCollection1[name_of_event].createFilter(fromBlock=fromBlock, toBlock=toBlock-1)
                events = event_filter.get_all_entries()

                for i in range(len(events)):
                    df = pd.json_normalize(dict(dict(events[i])['args']))
                    df['blocknumber'] = dict(events[i])['blockNumber']
                    df['txhash'] = dict(events[i])['transactionHash'].hex()
                    if 'contextId' in df.columns:
                        df.loc[:,'contextId'] = [w3.toHex(x) for x in df.contextId]
                    else:
                        pass
                    df = df.astype(str)  
                    collectedData = collectedData.append(df)
            except: 
                pass
        collectedData.name = f'{str(name_of_event)}'
        
        blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
        
        if len(collectedData) == 0:
            pass
        else:
            collectedData.blocknumber = collectedData.blocknumber.astype(int)

            if 'time' not in collectedData.columns:
                newcollectedData = pd.merge(collectedData, blockNumber_to_timestamp, on='blocknumber', how='left')
            else:
                newcollectedData = collectedData.copy()
            for label in ['pool', 'token', 'sourceToken', 'targetToken', 'rewardsToken']:
                if label in newcollectedData.columns:
                    symbollabel = label.replace('Token','')+"Symbol"
                    newcollectedData.loc[:,symbollabel] = [tokenSymbols[x] for x in newcollectedData.loc[:,label]]
                    decimallabel = label.replace('Token','')+"Decimals"
                    newcollectedData.loc[:,decimallabel] = [tokenDecimals[x] for x in newcollectedData.loc[:,symbollabel]]
            collectedData = newcollectedData.copy()

        collectedData = collectedData.astype(str)
        master = master.astype(str)

        master = master.append(collectedData)
        master = master[~master.duplicated()].copy()
        master.reset_index(inplace=True, drop=True)
        master = master.astype(str)
        master.to_parquet(file_location_par, compression='gzip')

    return()

# COMMAND ----------

def update_TEMPLATE_events(contract, name_of_event):
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    toBlock = int(blockNumber_to_timestamp.blockNumber.max())
    name_of_contract = contract['name']
        
    file_location = ETL_CSV_STORAGE_DIRECTORY+f'Events_{str(name_of_contract)}_{str(name_of_event)}.parquet'
    file_location_par = ETL_CSV_STORAGE_DIRECTORY+f'Events_{str(name_of_contract)}_{str(name_of_event)}.parquet'
    if os.path.isfile(file_location):
        master = pd.read_parquet(file_location)
        if len(master) != 0:
            master.loc[:,'blocknumber'] = [int(float(x)) for x in master.blocknumber]
            master.loc[:,'timestamp'] = [int(float(x)) for x in master.timestamp]
            fromBlock = int(master.blocknumber.max())
        else:
            fromBlock = 14609000
            master = pd.DataFrame()
    else:
        fromBlock = 14609000
        master = pd.DataFrame()
    
    event_filter = contract[f'{name_of_event}'].createFilter(fromBlock=fromBlock+1, toBlock=toBlock-1)
    events = event_filter.get_all_entries()

    collectedData = pd.DataFrame()
    for i in range(len(events)):
        df = pd.json_normalize(dict(dict(events[i])['args']))
        df['blocknumber'] = dict(events[i])['blockNumber']
        df['txhash'] = dict(events[i])['transactionHash'].hex()
        if 'contextId' in df.columns:
            df.loc[:,'contextId'] = [w3.toHex(x) for x in df.contextId]
        else:
            pass
        df = df.astype(str)  
        collectedData = collectedData.append(df)
    collectedData.name = f'{str(name_of_event)}'

    blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
    
    if len(collectedData) == 0:
        pass
    else:
        collectedData.blocknumber = collectedData.blocknumber.astype(int)

        if 'time' not in collectedData.columns:
            newcollectedData = pd.merge(collectedData, blockNumber_to_timestamp, on='blocknumber', how='left')
        else:
            newcollectedData = collectedData.copy()
        for label in ['pool', 'token', 'sourceToken', 'targetToken', 'rewardsToken']:
            if label in newcollectedData.columns:
                symbollabel = label.replace('Token','')+"Symbol"
                newcollectedData.loc[:,symbollabel] = [tokenSymbols[x] for x in newcollectedData.loc[:,label]]
                decimallabel = label.replace('Token','')+"Decimals"
                newcollectedData.loc[:,decimallabel] = [tokenDecimals[x] for x in newcollectedData.loc[:,symbollabel]]
        collectedData = newcollectedData.copy()

    if "_amount" in collectedData.columns:
        collectedData.loc[:,'amount'] = collectedData.loc[:,'_amount']
        collectedData.drop('_amount', axis=1, inplace=True)
        
    if "_amount" in master.columns:
        master.loc[:,'amount'] = master.loc[:,'_amount']
        master.drop('_amount', axis=1, inplace=True)
    
    collectedData = collectedData.astype(str)
    master = master.astype(str)
    
    master = master.append(collectedData)
    master = master[~master.duplicated()].copy()
    master.reset_index(inplace=True, drop=True)
    master = master.astype(str)
    master.to_parquet(file_location_par, compression='gzip')

    return(master)

# COMMAND ----------

# DBTITLE 1,Update Events
for event_group in events_list:
    print(event_group[0]['name'], event_group[1])
    update_TEMPLATE_events(event_group[0], event_group[1])

# COMMAND ----------

update_PoolCollection_events()

# COMMAND ----------

# DBTITLE 1,Create *_real amounts etc.
# Specific for PC TokensWithdrawn
for stringdf in ["Events_PoolCollection_TokensWithdrawn"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    df.loc[:,'tokenAmount_real'] = [Decimal(df.loc[i,'tokenAmount']) / Decimal('10')**Decimal(df.tokenDecimals[i]) for i in df.index]
    df.loc[:,'poolTokenAmount_real'] = [Decimal(df.loc[i,'poolTokenAmount']) / Decimal('10')**Decimal(df.tokenDecimals[i]) for i in df.index]
    df.loc[:,'baseTokenAmount_real'] = [Decimal(df.loc[i,'baseTokenAmount']) / Decimal('10')**Decimal(df.tokenDecimals[i]) for i in df.index]
    df.loc[:,'externalProtectionBaseTokenAmount_real'] = [Decimal(df.loc[i,'externalProtectionBaseTokenAmount']) / Decimal('10')**Decimal(df.tokenDecimals[i]) for i in df.index]
    df.loc[:,'bntAmount_real'] = [Decimal(x) / Decimal('10')**Decimal('18') for x in df.loc[:,"bntAmount"]]
    df.loc[:,'withdrawalFeeAmount_real'] = [Decimal(df.loc[i,'withdrawalFeeAmount']) / Decimal('10')**Decimal(df.tokenDecimals[i]) for i in df.index]
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')

# Specific for BN TokensTraded
for stringdf in ["Events_BancorNetwork_TokensTraded"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    df.loc[:,'sourceAmount_real'] = [Decimal(df.loc[i,'sourceAmount']) / Decimal('10')**Decimal(df.sourceDecimals[i]) for i in df.index]
    df.loc[:,'targetAmount_real'] = [Decimal(df.loc[i,'targetAmount']) / Decimal('10')**Decimal(df.targetDecimals[i]) for i in df.index]
    df.loc[:,'targetFeeAmount_real'] = [Decimal(df.loc[i,'targetFeeAmount']) / Decimal('10')**Decimal(df.targetDecimals[i]) for i in df.index]
    df.loc[:,'bntAmount_real'] = [Decimal(x) / Decimal('10')**Decimal('18') for x in df.loc[:,"bntAmount"]]
    df.loc[:,'bntFeeAmount_real'] = [Decimal(x) / Decimal('10')**Decimal('18') for x in df.loc[:,"bntFeeAmount"]]
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')

# Specific for rewards to rewardsDecimals
for stringdf in ["Events_StandardRewards_ProgramCreated"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    for col in df.columns:
        if "_real" not in col:
            for label in ['totalRewards']:
                if label in col:
                    df.loc[:,f'{col}_real'] = [Decimal(df.loc[i,col]) / Decimal('10')**Decimal(df.rewardsDecimals[i]) for i in df.index]
        else:
            pass
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')
    
# Specific for rewards to 18
for stringdf in ["Events_StandardRewards_ProgramEnabled", "Events_StandardRewards_ProgramTerminated"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    for col in df.columns:
        if "_real" not in col:
            for label in ["remainingRewards", ]:
                if label in col:
                    df.loc[:,f'{col}_real'] = [Decimal(x) / Decimal('10')**Decimal('18') for x in df.loc[:,col]]
        else:
            pass
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')
    
    
# When many amount map to tokenDecimals
for stringdf in ["Events_BancorNetwork_FundsMigrated", "Events_ExternalProtectionVault_FundsWithdrawn", "Events_MasterVault_FundsBurned", "Events_MasterVault_FundsWithdrawn", "Events_PoolCollection_TokensDeposited", "Events_PoolCollection_TradingLiquidityUpdated"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    for col in df.columns:
        if "_real" not in col:
            for label in ['mount', 'iquidity']:
                if label in col:
                    df.loc[:,f'{col}_real'] = [Decimal(df.loc[i,col]) / Decimal('10')**Decimal(df.tokenDecimals[i]) for i in df.index]
        else:
            pass
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')
    
# When many amount map to poolDecimals
for stringdf in ["Events_PoolCollection_TotalLiquidityUpdated", "Events_StandardRewards_ProviderJoined", "Events_StandardRewards_ProviderLeft", "Events_PendingWithdrawals_WithdrawalInitiated", "Events_PendingWithdrawals_WithdrawalCompleted", "Events_PendingWithdrawals_WithdrawalCancelled"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    for col in df.columns:
        if "_real" not in col:
            for label in ['mount', 'iquidity', 'stakedBalance', 'poolTokenSupply', "Limit", 'remainingStake']:
                if label in col:
                    df.loc[:,f'{col}_real'] = [Decimal(df.loc[i,col]) / Decimal('10')**Decimal(df.poolDecimals[i]) for i in df.index]
        else:
            pass    
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')
    
# When all decimals are 18
for stringdf in ["Events_BNTPool_FundingRenounced", "Events_BNTPool_FundingRequested", "Events_BNTPool_TokensDeposited", "Events_BNTPool_TokensWithdrawn", "Events_BNTPool_TotalLiquidityUpdated",
                 "Events_NetworkSettings_FundingLimitUpdated", "Events_NetworkSettings_MinLiquidityForTradingUpdated", "Events_NetworkSettings_VortexBurnRewardUpdated", "Events_StakingRewardsClaim_RewardsClaimed",
                "Events_StakingRewardsClaim_RewardsStaked", "Events_StandardRewards_RewardsClaimed", "Events_StandardRewards_RewardsStaked", "Events_BNT_Issuance","Events_BNT_Destruction","Events_VBNT_Issuance","Events_VBNT_Destruction",]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    for col in df.columns:
        if "_real" not in col:
            for label in ['mount', 'iquidity', 'stakedBalance', 'poolTokenSupply', "Limit"]:
                if label in col:
                    df.loc[:,f'{col}_real'] = [Decimal(x) / Decimal('10')**Decimal('18') for x in df.loc[:,col]]
        else:
            pass
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')

# COMMAND ----------

eventsfiles = glob.glob(ETL_CSV_STORAGE_DIRECTORY+'Events_**')
eventsfiles = [x for x in eventsfiles if 'parquet' in x]
print(len(eventsfiles))
eventsfiles

# COMMAND ----------

def repair_missing_times(eventsfiles):
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
    blockNumber_to_timestamp.blocknumber = blockNumber_to_timestamp.blocknumber.astype(int)
    filter_out = [ETL_CSV_STORAGE_DIRECTORY+'Events_poolData_Historical_latest.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_v3_daily_bntTradingLiquidity.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_deficit_by_tkn.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_spotRates_emaRates.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_tradingLiquidity.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_BNT_Issuance.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_BNT_Destruction.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_VBNT_Issuance.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_VBNT_Destruction.parquet', ETL_CSV_STORAGE_DIRECTORY+'Events_BNT_VBNT_DailyTokenSupply.parquet',]
    eventsfiles2 = [x for x in eventsfiles if x not in filter_out]
    for file in eventsfiles2:
        print(file)
        Events_df = pd.read_parquet(file)
        if 'blocknumber' in Events_df.columns:
    #             print(file)
            Events_df2 = Events_df.copy()
            Events_df2.loc[:,'blocknumber'] = [int(float(x)) for x in Events_df2.blocknumber]
            Events_df2.loc[:,'timestamp'] = [int(float(x)) for x in Events_df2.timestamp]
            Events_df2.timestamp.fillna('0', inplace= True)
            missingdf = Events_df2[Events_df2.timestamp=='0'].copy()
            missingindex = list(missingdf.index)

            if len(missingindex) != 0:
                print(len(missingindex))
                mdf = pd.DataFrame()
                for i in missingindex:
                    df = blockNumber_to_timestamp[blockNumber_to_timestamp.blocknumber==missingdf.blocknumber[i]][['time','timestamp']].copy()
                    mdf = mdf.append(df)
                mdf.index = missingindex

                missingdf.drop(['time', 'timestamp'], axis=1, inplace=True)
                replacement = pd.concat([missingdf, mdf], axis=1)

                Events_df2.drop(missingindex, inplace=True)
                Events_df2 = Events_df2.append(replacement)
                Events_df2.sort_index(inplace=True)
                Events_df2 = Events_df2[~Events_df2.duplicated()].copy()
                Events_df2.reset_index(inplace=True, drop=True)
                Events_df2 = Events_df2.astype(str)
                Events_df2.to_parquet(file, compression='gzip')
                print(f"Updated and saved: {file}")
            else:
                pass
        else:
            pass

# COMMAND ----------

repair_missing_times(eventsfiles)

# COMMAND ----------

def get_updatePriceData(tokenSymbol):
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
    
    tokenSymbol = tokenSymbol.upper()
    print(f"Updating {tokenSymbol} prices...")
    mdf = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'HistoricalPriceData_{tokenSymbol}.parquet')
    mdf.loc[:,'time'] = [int(float(x)) for x in mdf.time]
    final = blockNumber_to_timestamp.timestamp.iloc[-1]
    init = mdf.time.iloc[-1]

    r = requests.get(f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={tokenSymbol}&tsym=USD&limit=2000&toTs={final}&api_key={ETL_CRYPTOCOMPARE_APIKEY}")
    TimeTo = r.json()['Data']['TimeTo']
    TimeFrom = r.json()['Data']['TimeFrom']
    df = pd.DataFrame(r.json()['Data']['Data'])
    df['tokenSymbol'] = tokenSymbol
    mdf = mdf.append(df)
    mdf.sort_values(by='time', inplace=True)  

    while TimeFrom > init:
        r = requests.get(f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={tokenSymbol}&tsym=USD&limit=2000&toTs={TimeFrom}&api_key={ETL_CRYPTOCOMPARE_APIKEY}")
        if r.json()['Data'] == {}:
            pass
        else:
            TimeTo = r.json()['Data']['TimeTo']
            TimeFrom = r.json()['Data']['TimeFrom']
            df = pd.DataFrame(r.json()['Data']['Data'])
            df['tokenSymbol'] = tokenSymbol
            mdf = mdf.append(df)
            mdf.sort_values(by='time', inplace=True)


    mdf = mdf[~mdf.duplicated()].copy()
    mdf.reset_index(inplace=True, drop=True)
    mdf = mdf.astype(str)
    mdf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f'HistoricalPriceData_{tokenSymbol}.parquet', compression='gzip')
    print(len(mdf))

# COMMAND ----------

get_updatePriceData('bnt')

# COMMAND ----------

def update_daily_bntprices():
    from_time = datetime.datetime.fromisoformat('2020-01-01')
    to_time = datetime.datetime.now()
    from_time_timestamp = int(from_time.timestamp())
    to_time_timestamp = int(to_time.timestamp())

    url = f"https://api.coingecko.com/api/v3/coins/bancor/market_chart/range"
    params = {"vs_currency": 'usd', "from": from_time_timestamp, "to": to_time_timestamp}
    headers = {'user-agent':'C'}
    r = requests.get(url=url, params=params, headers=headers).json()
    prices = pd.DataFrame(r['prices'], columns = ['timestamp', 'bntprice'])
    prices.loc[:,'timestamp'] = [int(x/1000) for x in prices.timestamp]
    prices.loc[:,'day'] = [datetime.datetime.fromtimestamp(x) for x in prices.timestamp]
    prices.set_index('day', inplace=True)
    prices = prices.astype(str)
    prices.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'cg_daily_bntprices.parquet', compression='gzip')

# COMMAND ----------

update_daily_bntprices()

# COMMAND ----------

def create_historical_pool_spotrates():
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
    
    bntprices = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'HistoricalPriceData_BNT.parquet')
    bntprices = bntprices[['time', 'close']].copy()
    bntprices.columns = ['timestamp','price']
    bntprices.loc[:,'timestamp'] = [int(float(x)) for x in bntprices.timestamp]
    bntprices.loc[:,'price'] = [Decimal(x) for x in bntprices.price]
    
    Events_PoolCollection_TradingLiquidityUpdated = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_PoolCollection_TradingLiquidityUpdated.parquet")
    for col in ['newLiquidity_real']:
        Events_PoolCollection_TradingLiquidityUpdated.loc[:,col] = Events_PoolCollection_TradingLiquidityUpdated.loc[:,col].apply(lambda x: Decimal(str(x)))
    Events_PoolCollection_TradingLiquidityUpdated.blocknumber = Events_PoolCollection_TradingLiquidityUpdated.blocknumber.astype(int)
        
    tl = Events_PoolCollection_TradingLiquidityUpdated[['pool', 'poolSymbol', 'tokenSymbol', 'newLiquidity_real', 'blocknumber',]].copy()
    tltkn = tl[tl.tokenSymbol!='bnt'].copy()
    tlbnt = tl[tl.tokenSymbol=='bnt'].copy()

    tltkn.columns = ['pool', 'poolSymbol', 'tokenSymbol', 'tknnewLiquidity', 'blocknumber',]
    tlbnt.columns = ['pool', 'poolSymbol', 'tokenSymbol', 'bntnewLiquidity', 'blocknumber',]

    blocknums = pd.DataFrame(range(tl.blocknumber.min(), blockNumber_to_timestamp.blocknumber.max()))
    blocknums.columns = ['blocknumber']

    mtl = pd.DataFrame()
    for poolSymbol in list(tl.poolSymbol.unique()):
        tkndf = tltkn[tltkn.poolSymbol==poolSymbol].copy()
        bntdf = tlbnt[tlbnt.poolSymbol==poolSymbol].copy()
        newtl = pd.merge(blocknums, tkndf, how='left', on='blocknumber')
        newtl = newtl[~newtl.duplicated()].copy()
        newtl2 = pd.merge(newtl, bntdf[['blocknumber','bntnewLiquidity']], how='left', on='blocknumber')
        newtl2 = newtl2[~newtl2.duplicated()].copy()
        newtl2.fillna(method='ffill', inplace=True)
        sub = newtl2[newtl2.blocknumber.isin(tl.blocknumber.unique())].copy()
        mtl = mtl.append(sub)

    mtl.reset_index(inplace=True, drop=True)
    mtl.fillna('0', inplace=True)
    mtl.drop(mtl[mtl.pool=='0'].index, inplace=True)
    mtl.reset_index(inplace=True, drop=True)
        
    spotrates = []
    for i in mtl.index:
        if mtl.tknnewLiquidity[i] == 0:
            spotrates += [Decimal('0')]
        else:
            spotrates += [mtl.bntnewLiquidity[i] / mtl.tknnewLiquidity[i]]

    mtl.loc[:,'spotRate'] = spotrates
    mtl.rename(columns = {'tknnewLiquidity':'tknTradingLiquidity_real','bntnewLiquidity':'bntTradingLiquidity_real'}, inplace=True)
    mtl.drop(['tokenSymbol'], axis=1, inplace=True)
    mtl = pd.merge(mtl, blockNumber_to_timestamp, how='left', on = 'blocknumber')
    mtl = mtl[~mtl.duplicated()].copy()
    mtl.sort_values(by='blocknumber', inplace=True)
    mtl.reset_index(inplace=True, drop=True)
    
    newmtl = pd.merge(mtl, bntprices, how='left', on='timestamp') 
    newmtl.loc[0,'price'] = newmtl.loc[1,'price']
    newmtl.price.fillna(method='ffill', inplace=True)
    newmtl = newmtl[~newmtl.duplicated()].copy()
    newmtl.reset_index(inplace=True, drop=True)

    newmtl = newmtl.astype(str)
    newmtl.to_parquet(ETL_CSV_STORAGE_DIRECTORY+"PoolCollection_TradingLiquidityUpdated_SpotRates.parquet", compression='gzip')

# COMMAND ----------

create_historical_pool_spotrates()

# COMMAND ----------

def create_updated_TokensTraded_table():
    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.loc[:,'blockNumber'] = [int(float(x)) for x in blockNumber_to_timestamp.blockNumber]
    blockNumber_to_timestamp.loc[:,'timestamp'] = [int(float(x)) for x in blockNumber_to_timestamp.timestamp]
    blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
    
    bntprices = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'HistoricalPriceData_BNT.parquet')
    bntprices = bntprices[['time', 'close']].copy()
    bntprices.loc[:,'time'] = [int(float(x)) for x in bntprices.time]
    bntprices.set_index('time', inplace=True)
    bntprices.columns = ['price']
    bntprices.loc[:,'price'] = [Decimal(x) for x in bntprices.price]
    
    Events_BancorNetwork_TokensTraded = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_BancorNetwork_TokensTraded.parquet")
    for col in ['sourceAmount_real',  'targetAmount_real', 'targetFeeAmount_real','bntAmount_real', 'bntFeeAmount_real']:
        Events_BancorNetwork_TokensTraded.loc[:,col] = Events_BancorNetwork_TokensTraded.loc[:,col].apply(lambda x: Decimal(str(x)))
    Events_BancorNetwork_TokensTraded.blocknumber = Events_BancorNetwork_TokensTraded.blocknumber.astype(int)
    Events_BancorNetwork_TokensTraded.timestamp = [int(float(x)) for x in Events_BancorNetwork_TokensTraded.timestamp]
    
    # create a useful dictionary to parse the spotrates
    TL_dict = {}
    mtl2 = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+"PoolCollection_TradingLiquidityUpdated_SpotRates.parquet")
    for col in ['spotRate']:
        mtl2.loc[:,col] = mtl2.loc[:,col].apply(lambda x: Decimal(str(x)))
    mtl2.blocknumber = mtl2.blocknumber.astype(int)

    for poolSymbol in mtl2.poolSymbol.unique():
        sub = mtl2[mtl2.poolSymbol==poolSymbol].copy()
        sub.drop(['poolSymbol'], axis=1, inplace=True)
        TL_dict[poolSymbol] = {x: list(sub.loc[:,x]) for x in sub.columns}

    spotrate_of_source = []
    spotrate_of_target = []
    for i in Events_BancorNetwork_TokensTraded.index:
        sourceSymbol = Events_BancorNetwork_TokensTraded.sourceSymbol[i]
        targetSymbol = Events_BancorNetwork_TokensTraded.targetSymbol[i]
        blocknumber = Events_BancorNetwork_TokensTraded.blocknumber[i]
        if sourceSymbol != 'bnt':
            recentblockindex = [i for (i,c) in list(enumerate(TL_dict[sourceSymbol]['blocknumber'])) if c <= blocknumber][-1]
            spotrate_of_source += [TL_dict[sourceSymbol]['spotRate'][recentblockindex]]
        else:
            spotrate_of_source += [Decimal('1')]

        if targetSymbol != 'bnt':
            recentblockindex = [i for (i,c) in list(enumerate(TL_dict[targetSymbol]['blocknumber'])) if c <= blocknumber][-1]
            spotrate_of_target += [TL_dict[targetSymbol]['spotRate'][recentblockindex]]
        else:
            spotrate_of_target += [Decimal('1')]

    Events_BancorNetwork_TokensTraded.loc[:,'sourceSpotRate_bnt'] = spotrate_of_source
    Events_BancorNetwork_TokensTraded.loc[:,'targetSpotRate_bnt'] = spotrate_of_target

    Events_BancorNetwork_TokensTraded.loc[:,'sourceAmount_real_bnt'] = Events_BancorNetwork_TokensTraded.sourceAmount_real * Events_BancorNetwork_TokensTraded.sourceSpotRate_bnt
    Events_BancorNetwork_TokensTraded.loc[:,'targetAmount_real_bnt'] = Events_BancorNetwork_TokensTraded.targetAmount_real * Events_BancorNetwork_TokensTraded.targetSpotRate_bnt
    Events_BancorNetwork_TokensTraded.loc[:,'targetFeeAmount_real_bnt'] = Events_BancorNetwork_TokensTraded.targetFeeAmount_real * Events_BancorNetwork_TokensTraded.targetSpotRate_bnt
    Events_BancorNetwork_TokensTraded.loc[:,'bntAmount_real_bnt'] = Events_BancorNetwork_TokensTraded.bntAmount_real   #already in bnt
    Events_BancorNetwork_TokensTraded.loc[:,'bntFeeAmount_real_bnt'] = Events_BancorNetwork_TokensTraded.bntFeeAmount_real   #already in bnt
    
    actualfees = []
    for i in Events_BancorNetwork_TokensTraded.index:
        if Events_BancorNetwork_TokensTraded.targetSymbol[i] =='bnt':
            actualfees += [Events_BancorNetwork_TokensTraded.targetFeeAmount_real_bnt[i]]
        else:
            actualfees += [Events_BancorNetwork_TokensTraded.targetFeeAmount_real_bnt[i] + Events_BancorNetwork_TokensTraded.bntFeeAmount_real_bnt[i]]
    Events_BancorNetwork_TokensTraded['actualTotalFees_real_bnt'] = actualfees
    
    Events_BancorNetwork_TokensTraded.loc[:,'bntprice'] = [Decimal(str(bntprices[bntprices['price'].index<=timestamp].iloc[-1]['price'])) for timestamp in Events_BancorNetwork_TokensTraded.timestamp]
    
    Events_BancorNetwork_TokensTraded.loc[:,'sourceAmount_real_usd'] = Events_BancorNetwork_TokensTraded.sourceAmount_real_bnt * Events_BancorNetwork_TokensTraded.bntprice
    Events_BancorNetwork_TokensTraded.loc[:,'targetAmount_real_usd'] = Events_BancorNetwork_TokensTraded.targetAmount_real_bnt * Events_BancorNetwork_TokensTraded.bntprice
    Events_BancorNetwork_TokensTraded.loc[:,'targetFeeAmount_real_usd'] = Events_BancorNetwork_TokensTraded.targetFeeAmount_real_bnt * Events_BancorNetwork_TokensTraded.bntprice
    Events_BancorNetwork_TokensTraded.loc[:,'bntAmount_real_usd'] = Events_BancorNetwork_TokensTraded.bntAmount_real_bnt * Events_BancorNetwork_TokensTraded.bntprice
    Events_BancorNetwork_TokensTraded.loc[:,'bntFeeAmount_real_usd'] = Events_BancorNetwork_TokensTraded.bntFeeAmount_real_bnt * Events_BancorNetwork_TokensTraded.bntprice
    Events_BancorNetwork_TokensTraded.loc[:,'actualTotalFees_real_usd'] = Events_BancorNetwork_TokensTraded.actualTotalFees_real_bnt * Events_BancorNetwork_TokensTraded.bntprice

    Events_BancorNetwork_TokensTraded = Events_BancorNetwork_TokensTraded.astype(str)
    Events_BancorNetwork_TokensTraded.to_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_BancorNetwork_TokensTraded_Updated.parquet", compression='gzip')

# COMMAND ----------

create_updated_TokensTraded_table()

# COMMAND ----------

# DBTITLE 1,V3 Historical PoolData Stats
def update_daily_poolData_historical():
    maxpositionsdf = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'maxpositionsdf.parquet')  
    maxpositionsdf.blocknumber = maxpositionsdf.blocknumber.astype(int)
    maxpositionsdf.sort_values(by='blocknumber', ascending=False, inplace=True)
    maxpositionsdf.set_index('blocknumber', inplace=True)
    dayinfo = maxpositionsdf.copy()

    cgprices = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'cg_daily_bntprices.parquet') 

    all_files = glob.glob(ETL_CSV_STORAGE_DIRECTORY+f'poolData_Historical_20*')
    all_files = [x for x in all_files if 'parquet' in x]
    all_files = [x for x in all_files if 'latest' not in x]
    previous_day = all_files[-1].split('_')[-1].split('.')[0]
    unrecorded_days = [x for x in dayinfo.day if x>previous_day]

    for day in unrecorded_days:
        blocknumber = int(dayinfo[dayinfo.day==day].index.values[0])
        print(blocknumber, day)
        sdf = pd.DataFrame()

        vaultBalances = pd.DataFrame()
        pool_list = list(tokenAddresses.keys())
        pool_list = [x for x in pool_list if x not in ['bnt']]
        for tkn in pool_list:

            if tkn == 'eth':
                masterVault_tknBalance = w3.eth.getBalance(MasterVault['addy'], blocknumber)
            else:
                try:
                    masterVault_tknBalance = tokenContracts[tkn].functions.balanceOf(MasterVault['addy']).call(block_identifier=blocknumber)
                except:
                    masterVault_tknBalance = 0

            df = pd.DataFrame([masterVault_tknBalance],
                             columns = [tkn],
                             index = ['masterVault_tknBalance']).T

            vaultBalances = vaultBalances.append(df)

        tokenListnz = list(vaultBalances[vaultBalances.masterVault_tknBalance!=0].index)


        for tkn in tokenListnz:
            addy = BancorNetwork['contract'].functions.collectionByPool(tokenAddresses[tkn]).call(block_identifier = blocknumber)
            pooldata = PoolCollectionset[PoolCollectionAddys[addy]]['contract'].functions.poolData(tokenAddresses[tkn]).call(block_identifier=blocknumber)
            if addy in ["0x6f9124C32a9f6E532C908798F872d5472e9Cb714", "0xEC9596e0eB67228d61a12CfdB4b3608281F261b3",]: # V1, V2
                poolToken, tradingFeePPM, tradingEnabled, depositingEnabled, averageRate, depositLimit, tknPool_liquidity = pooldata
                bntTradingLiquidity, tknTradingLiquidity, stakingLedger_tknBalance = tknPool_liquidity
                ema_blocknum, ema_tuple = averageRate
                emaCompressedNumerator, emaCompressedDenominator = ema_tuple
                emaInvCompressedNumerator = Decimal('0')
                emaInvCompressedDenominator = Decimal('1')
            elif addy in ["0xF506B96891dDe3c149FF08b2FF26a059258f7eC7","0xAD3339099ae87f1ad6e984872B95E7be24b813A7","0xb8d8033f7B2267FEFfdBAA521Cd8a86DF861Da69","0x05E29F07B9710368A1D5658750e9B4B478c15bB8","0x395eD9ffd32b255dBD128092ABa40200159d664b", "0xD2a572fEfdbD719605334DF5CBA9746e02D51558","0x5cE51256651aA90eee24259a56529afFcf13a3d0", "0xd982e001491D414c857F2A1aaA4B43Ccf9f642B4",]: #V3, V4, V5, V6, V7, V8, V9, V10
                poolToken, tradingFeePPM, tradingEnabled, depositingEnabled, averageRate, tknPool_liquidity = pooldata
                depositLimit = Decimal('0')
                bntTradingLiquidity, tknTradingLiquidity, stakingLedger_tknBalance = tknPool_liquidity
                ema_blocknum, ema_tuple, emaInv_tuple = averageRate
                emaCompressedNumerator, emaCompressedDenominator = ema_tuple
                emaInvCompressedNumerator, emaInvCompressedDenominator = emaInv_tuple

            else:
                print('Unknown PoolCollection Addy')
                break

            if tkn == 'eth':
                externalProtectionVaultTknBalance = w3.eth.getBalance(ExternalProtectionVault['addy'], block_identifier=blocknumber)
            else:
                externalProtectionVaultTknBalance = tokenContracts[tkn].functions.balanceOf(ExternalProtectionVault['addy']).call(block_identifier=blocknumber)

            emaCompressedNumerator_real = Decimal(emaCompressedNumerator) / Decimal('10')**Decimal('18')
            emaCompressedDenominator_real = Decimal(emaCompressedDenominator) / Decimal('10')**Decimal(str(tokenDecimals[tkn]))
            emaInvCompressedNumerator_real = Decimal(emaInvCompressedNumerator) / Decimal('10')**Decimal(str(tokenDecimals[tkn]))
            emaInvCompressedDenominator_real = Decimal(emaInvCompressedDenominator) / Decimal('10')**Decimal('18')

            emaRate = emaCompressedNumerator_real/emaCompressedDenominator_real
            emaInvRate = emaInvCompressedNumerator_real/emaInvCompressedDenominator_real

            spotRate = get_spot_rate(bntTradingLiquidity, tknTradingLiquidity, str(tokenDecimals[tkn]))

            emaDeviation = get_emaDeviation(emaRate, spotRate)
            emaInvDeviation = get_emaInvDeviation(emaInvRate, spotRate)

            bntFundingLimit = NetworkSettings['contract'].functions.poolFundingLimit(tokenAddresses[tkn]).call(block_identifier=blocknumber)
            bntRemainingFunding  = BNTPool['contract'].functions.availableFunding(tokenAddresses[tkn]).call(block_identifier=blocknumber)
            bntFundingAmount = BNTPool['contract'].functions.currentPoolFunding(tokenAddresses[tkn]).call(block_identifier=blocknumber)

            masterVault_tknBalance = vaultBalances['masterVault_tknBalance'][tkn]

            df = pd.DataFrame([masterVault_tknBalance, tradingEnabled, bntTradingLiquidity, tknTradingLiquidity, stakingLedger_tknBalance,
                         tradingFeePPM, bntFundingLimit, bntRemainingFunding, bntFundingAmount, externalProtectionVaultTknBalance,
                          spotRate, ema_blocknum, emaRate, emaCompressedNumerator, emaCompressedDenominator, emaInvRate, emaInvCompressedNumerator, emaInvCompressedDenominator, 
                           depositingEnabled, emaDeviation,emaInvDeviation],
                         columns = [tkn],
                         index = ['masterVault_tknBalance', 'tradingEnabled', 'bntTradingLiquidity', 'tknTradingLiquidity', 'stakingLedger_tknBalance',
                         'tradingFeePPM', 'bntFundingLimit', 'bntRemainingFunding', 'bntFundingAmount', 'externalProtectionVaultTknBalance',
                          'spotRate', 'ema_blocknum', 'emaRate', 'emaCompressedNumerator', 'emaCompressedDenominator', 'emaInvRate', 'emaInvCompressedNumerator', 'emaInvCompressedDenominator', 
                          'depositingEnabled', 'emaDeviation','emaInvDeviation']).T

            df.rename(columns={
                        'masterVault_tknBalance': 'masterVaultTknBalance',
                        'stakingLedger_tknBalance': 'stakedBalance',
                        'ema_blocknum': 'emaBlockNumber',
            }, inplace=True)
            df.loc[:,'blocknumber'] = blocknumber
            df.loc[:,'day'] = day

            df = df[['blocknumber', 'day', 'bntTradingLiquidity', 'tknTradingLiquidity', 'masterVaultTknBalance', 'stakedBalance',
               'spotRate','emaRate', 'emaDeviation', 'emaInvRate', 'emaInvDeviation', 
               'tradingFeePPM', 'depositingEnabled', 'tradingEnabled', 
               'emaBlockNumber', 'emaCompressedNumerator','emaCompressedDenominator',  'emaInvCompressedNumerator', 'emaInvCompressedDenominator',
               'bntFundingLimit', 'bntRemainingFunding', 'bntFundingAmount', 'externalProtectionVaultTknBalance',
               ]].copy()

            sdf = sdf.append(df)

        ## BNT specific here ##

        masterVault_bntBalance = Decimal(tokenContracts['bnt'].functions.balanceOf(MasterVault['addy']).call(block_identifier=blocknumber))
        stakingLedger_bntBalance = Decimal(BNTPool['contract'].functions.stakedBalance().call(block_identifier=blocknumber))
        protocolWallet_bnbntBalance = Decimal(poolTokenContracts['bnbnt'].functions.balanceOf(BNTPool['addy']).call(block_identifier=blocknumber))
        vortexLedger_bntBalance = Decimal(BancorNetwork['contract'].functions.pendingNetworkFeeAmount().call(block_identifier=blocknumber))
        withdrawalFeePPM = Decimal(NetworkSettings['contract'].functions.withdrawalFeePPM().call(block_identifier=blocknumber))
        bnbntpoolTokenSupply = Decimal(poolTokenContracts[f'bnbnt'].functions.totalSupply().call(block_identifier=blocknumber))
        protocolWallet_bnbntPercentage = protocolWallet_bnbntBalance / bnbntpoolTokenSupply

        spotRate = Decimal('1')
        emaRate = Decimal('1')
        emaInvRate = Decimal('1')
        tradingFeePPM = Decimal('0')
        externalProtectionVaultTknBalance = Decimal('0')
        bntFundingLimit = Decimal('0')
        bntRemainingFunding = Decimal('0')
        bntFundingAmount = Decimal('0')
        tknTradingLiquidity = Decimal('0')
        bntTradingLiquidity = Decimal('0')
        vbntRate = sdf['spotRate']['vbnt']  

        bntdf = pd.DataFrame([(blocknumber, day, tknTradingLiquidity, bntTradingLiquidity, masterVault_bntBalance, stakingLedger_bntBalance, spotRate, emaRate, emaInvRate, tradingFeePPM, externalProtectionVaultTknBalance, bntFundingLimit, bntRemainingFunding, bntFundingAmount, vbntRate, protocolWallet_bnbntBalance, vortexLedger_bntBalance, withdrawalFeePPM, bnbntpoolTokenSupply, protocolWallet_bnbntPercentage)],
                    columns = ['blocknumber', 'day','tknTradingLiquidity', 'bntTradingLiquidity', 'masterVaultTknBalance', 'stakedBalance','spotRate', 'emaRate', 'emaInvRate', 'tradingFeePPM', 'externalProtectionVaultTknBalance', 'bntFundingLimit', 'bntRemainingFunding', 'bntFundingAmount', 'vbntRate', 'protocolWallet_bnbntBalance', 'vortexLedger_bntBalance', 'withdrawalFeePPM', 'bnbntpoolTokenSupply', 'protocolWallet_bnbntPercentage'],
                          index = ['bnt'])

        ## BNT specific here ##
        sdf = sdf.append(bntdf)

        sdf.loc[:,'bntprice'] = cgprices['bntprice'][day]

        for col in sdf.columns:
            if col not in ['day', 'depositingEnabled', 'tradingEnabled']:
                sdf[col] = sdf[col].apply(lambda x: Decimal(str(x)))
        sdf['decimals'] = [Decimal(str(tokenDecimals[x])) for x in sdf.index]
        sdf.reset_index(inplace=True)

        sdf.loc[:,'bntFundingLimit_real'] = [sdf.bntFundingLimit[i] / Decimal('10')**Decimal('18') for i in sdf.index]
        sdf.loc[:,'bntRemainingFunding_real'] = [sdf.bntRemainingFunding[i] / Decimal('10')**Decimal('18') for i in sdf.index]
        sdf.loc[:,'bntFundingAmount_real'] = [sdf.bntFundingAmount[i] / Decimal('10')**Decimal('18') for i in sdf.index]

        sdf.loc[:,'externalProtectionVaultTknBalance_real'] = [sdf.externalProtectionVaultTknBalance[i] / Decimal('10')**Decimal(sdf.decimals[i]) for i in sdf.index]
        sdf.loc[:,'externalProtectionVaultTknBalance_real_bnt'] = sdf.externalProtectionVaultTknBalance_real * sdf.emaRate
        sdf.loc[:,'externalProtectionVaultTknBalance_real_usd'] = sdf.externalProtectionVaultTknBalance_real_bnt * sdf.bntprice

        sdf.loc[:,'tradingFee'] = [sdf.tradingFeePPM[i] / Decimal('10')**Decimal('6') for i in sdf.index]
        sdf.loc[:,'withdrawalFee'] = [sdf.withdrawalFeePPM[i] / Decimal('10')**Decimal('6') for i in sdf.index]

        #spotRate for trading liquidity balance
        sdf.loc[:,'tknTradingLiquidity_real'] = [sdf.tknTradingLiquidity[i] / Decimal('10')**Decimal(sdf.decimals[i]) for i in sdf.index]
        sdf.loc[:,'tknTradingLiquidity_real_bnt'] = sdf.tknTradingLiquidity_real * sdf.spotRate
        sdf.loc[:,'tknTradingLiquidity_real_usd'] = sdf.tknTradingLiquidity_real_bnt * sdf.bntprice

        sdf.loc[:,'bntTradingLiquidity_real'] = [sdf.bntTradingLiquidity[i] / Decimal('10')**Decimal('18') for i in sdf.index]
        sdf.loc[:,'bntTradingLiquidity_real_bnt'] = sdf.bntTradingLiquidity_real  #direct copy to stay consistent with naming convention
        sdf.loc[:,'bntTradingLiquidity_real_usd'] = sdf.bntTradingLiquidity_real_bnt * sdf.bntprice

        # emaRate for vault and stake balances
        sdf.loc[:,'masterVaultTknBalance_real'] = [sdf.masterVaultTknBalance[i] / Decimal('10')**Decimal(sdf.decimals[i]) for i in sdf.index]
        sdf.loc[:,'masterVaultTknBalance_real_bnt'] = sdf.masterVaultTknBalance_real * sdf.emaRate
        sdf.loc[:,'masterVaultTknBalance_real_usd'] = sdf.masterVaultTknBalance_real_bnt * sdf.bntprice

        sdf.loc[:,'stakedBalance_real'] = [sdf.stakedBalance[i] / Decimal('10')**Decimal(sdf.decimals[i]) for i in sdf.index]
        sdf.loc[:,'stakedBalance_real_bnt'] = sdf.stakedBalance_real * sdf.emaRate
        sdf.loc[:,'stakedBalance_real_usd'] = sdf.stakedBalance_real_bnt * sdf.bntprice

        sdf.loc[:,'surplus_tkn'] = sdf.masterVaultTknBalance_real - sdf.stakedBalance_real
        sdf.loc[:,'surplus_bnt'] = sdf.surplus_tkn * sdf.emaRate
        sdf.loc[:,'surplus_usd'] = sdf.surplus_bnt * sdf.bntprice
        sdf.loc[:,'surplus_perc'] = [get_surplus_percent(sdf.masterVaultTknBalance_real[i], sdf.stakedBalance_real[i]) for i in sdf.index]

        sdf.loc[:,'protocolWallet_bnbntBalance_real'] = [sdf.protocolWallet_bnbntBalance[i] / Decimal('10')**Decimal('18') for i in sdf.index]
        sdf.loc[:,'vortexLedger_bntBalance_real'] = [sdf.vortexLedger_bntBalance[i] / Decimal('10')**Decimal('18') for i in sdf.index]
        sdf.loc[:,'bnbntpoolTokenSupply_real'] = [sdf.bnbntpoolTokenSupply[i] / Decimal('10')**Decimal('18') for i in sdf.index]

        sdf.rename(columns = {'index':'poolSymbol', 'day': 'time'}, inplace=True)
        sdf.fillna('0', inplace=True)
        sdf.replace('NaN', '0', inplace=True)
        sdf = sdf.astype(str)
        sdf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f'poolData_Historical_{day}.parquet', compression='gzip')


    # overwrite the latest data
    all_files = glob.glob(ETL_CSV_STORAGE_DIRECTORY+f'poolData_Historical_20*')
    all_files = [x for x in all_files if 'parquet' in x]
    all_files = [x for x in all_files if 'latest' not in x]
    df = pd.read_parquet(all_files[-1])
    df.rename(columns = {'symbol':'poolSymbol', 'day': 'time'}, inplace=True)
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f'Events_poolData_Historical_latest.parquet', compression='gzip')

# COMMAND ----------

update_daily_poolData_historical()

# COMMAND ----------

def get_v3_historical_deficit():
    all_files = glob.glob(ETL_CSV_STORAGE_DIRECTORY+f'*poolData_Historical_20*')
    all_files = [x for x in all_files if 'parquet' in x]
    all_files = [x for x in all_files if 'latest' not in x]
    infor = []
    for file in all_files:
        df = pd.read_parquet(file)
        df = df[df.poolSymbol != 'bnt'].copy()
        df.loc[:,'surplus_bnt'] = [Decimal(x) for x in df.surplus_bnt]
        df.loc[:,'surplus_usd'] = [Decimal(x) for x in df.surplus_usd]
        infor += [(df.iloc[0]['time'] ,df.surplus_bnt.sum(numeric_only=False), df.surplus_usd.sum(numeric_only=False))]
    infordf = pd.DataFrame(infor, columns = ['time', 'v3_surplus_bnt', 'v3_surplus_usd'])
    infordf = infordf.astype(str)
    infordf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_deficit.parquet', compression='gzip')

# COMMAND ----------

get_v3_historical_deficit()

# COMMAND ----------

def get_v3_historical_deficit_by_tkn():
    all_files = glob.glob(ETL_CSV_STORAGE_DIRECTORY+f'*poolData_Historical_20*')
    all_files = [x for x in all_files if 'parquet' in x]
    all_files = [x for x in all_files if 'latest' not in x]
    mdf = pd.DataFrame()
    for file in all_files:
        df = pd.read_parquet(file)
        df = df[df.poolSymbol != 'bnt'].copy()
        df2 = df[['poolSymbol','blocknumber','time','bntprice', 'surplus_tkn','surplus_bnt','surplus_usd','surplus_perc']].copy()
        mdf = mdf.append(df2)
    mdf = mdf.astype(str)
    mdf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_deficit_by_tkn.parquet', compression='gzip')

# COMMAND ----------

get_v3_historical_deficit_by_tkn()

# COMMAND ----------

def get_v3_daily_TL():
    all_files = glob.glob(ETL_CSV_STORAGE_DIRECTORY+f'*poolData_Historical_20*')
    all_files = [x for x in all_files if 'parquet' in x]
    all_files = [x for x in all_files if 'latest' not in x]
    historical_tradingLiquidity = pd.DataFrame()
    mdf = pd.DataFrame()
    for file in all_files:
        df = pd.read_parquet(file)
        df = df[['blocknumber', 'time', 'poolSymbol', 'bntTradingLiquidity_real', 'bntTradingLiquidity_real_bnt', 'bntTradingLiquidity_real_usd', 'tknTradingLiquidity_real', 'tknTradingLiquidity_real_bnt', 'tknTradingLiquidity_real_usd' ]].copy()
        historical_tradingLiquidity = historical_tradingLiquidity.append(df)

        df2 = df[['time', 'blocknumber', 'bntTradingLiquidity_real_bnt', 'bntTradingLiquidity_real_usd']].copy()
        mdf = mdf.append(df2)

    historical_tradingLiquidity = historical_tradingLiquidity.astype(str)
    historical_tradingLiquidity.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_tradingLiquidity.parquet', compression='gzip')

    mdf.loc[:,'bntTradingLiquidity_real_bnt'] = [Decimal(x) for x in mdf.bntTradingLiquidity_real_bnt]
    mdf.loc[:,'bntTradingLiquidity_real_usd'] = [Decimal(x) for x in mdf.bntTradingLiquidity_real_usd]
    mdfa = mdf[['time', 'bntTradingLiquidity_real_bnt', 'bntTradingLiquidity_real_usd']].groupby('time').sum(numeric_only=False).reset_index()
    mdfb = mdf[['time', 'blocknumber']].groupby('time').first().reset_index()
    historical_tradingLiquidity_sums = pd.merge(mdfb, mdfa, how='left', on = 'time')
    historical_tradingLiquidity_sums[~historical_tradingLiquidity_sums.duplicated()].copy()
    historical_tradingLiquidity_sums.reset_index(inplace=True, drop=True)
    historical_tradingLiquidity_sums.loc[:,'bntprice'] = [round(get_safe_divide(historical_tradingLiquidity_sums.bntTradingLiquidity_real_usd[i], historical_tradingLiquidity_sums.bntTradingLiquidity_real_bnt[i]),6) for i in historical_tradingLiquidity_sums.index]

    historical_tradingLiquidity_sums = historical_tradingLiquidity_sums.astype(str)
    historical_tradingLiquidity_sums.to_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_v3_daily_bntTradingLiquidity.parquet", compression='gzip')

# COMMAND ----------

get_v3_daily_TL()

# COMMAND ----------

def get_v3_daily_spotRates_emaRates():
    cgprices = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'cg_daily_bntprices.parquet') 
    cgprices.reset_index(inplace=True)
    cgprices.loc[:,'bntprice'] = [Decimal(x) for x in cgprices.bntprice]
    cgprices.loc[:,'day'] = [str(x)[:10] for x in cgprices.day]

    all_files = glob.glob(ETL_CSV_STORAGE_DIRECTORY+f'*poolData_Historical_20*')
    all_files = [x for x in all_files if 'parquet' in x]
    all_files = [x for x in all_files if 'latest' not in x]
    mdf = pd.DataFrame()
    for file in all_files:
        df = pd.read_parquet(file)
        df = df[['blocknumber', 'time', 'poolSymbol', 'spotRate', 'emaRate']].copy()
        mdf = mdf.append(df)

    mdf.loc[:,'spotRate'] = [Decimal(x) for x in mdf.spotRate]
    mdf.loc[:,'emaRate'] = [Decimal(x) for x in mdf.emaRate]
    mdf.loc[:,'time'] = [str(x) for x in mdf.time]
    mdf = pd.merge(mdf, cgprices, how='left', left_on='time', right_on='day')
    mdf.loc[:,'spotRate_usd'] = mdf.spotRate * mdf.bntprice
    mdf.loc[:,'emaRate_usd'] = mdf.emaRate * mdf.bntprice
    mdf = mdf[~mdf.duplicated()].copy()
    mdf.drop(['timestamp', 'day'],axis=1, inplace=True)
    mdf.reset_index(inplace=True, drop=True)
    mdf = mdf.astype(str)
    mdf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_v3_historical_spotRates_emaRates.parquet', compression='gzip')

# COMMAND ----------

get_v3_daily_spotRates_emaRates()

# COMMAND ----------

# DBTITLE 1,Generate Withdrawals Stats
WithdrawalInitiated = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_PendingWithdrawals_WithdrawalInitiated.parquet')
WithdrawalCancelled = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_PendingWithdrawals_WithdrawalCancelled.parquet')
WithdrawalCompleted = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_PendingWithdrawals_WithdrawalCompleted.parquet')

WithdrawalCurrentPending = WithdrawalInitiated[WithdrawalInitiated.requestId.isin(set(WithdrawalInitiated.requestId) - set(WithdrawalCompleted.requestId) - set(WithdrawalCancelled.requestId))].copy()
WithdrawalCurrentPending.reset_index(inplace=True, drop=True)
WithdrawalCurrentPending = WithdrawalCurrentPending.astype(str)
WithdrawalCurrentPending.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_PendingWithdrawals_WithdrawalCurrentPending.parquet', compression='gzip')

emarates = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_poolData_Historical_latest.parquet')
emarates = emarates[['poolSymbol','emaRate','bntprice']].copy()
emarates.loc[:,'emaRate'] = [Decimal(x) for x in emarates.emaRate]
emarates.loc[:,'bntprice'] = [Decimal(x) for x in emarates.bntprice]

for stringdf in ["Events_PendingWithdrawals_WithdrawalInitiated", "Events_PendingWithdrawals_WithdrawalCancelled", "Events_PendingWithdrawals_WithdrawalCompleted", "Events_PendingWithdrawals_WithdrawalCurrentPending"]:
    df = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet")
    df.drop(['emaRate', 'bntprice', 'reserveTokenAmount_real_bnt', 'reserveTokenAmount_real_usd'], axis=1, inplace=True)
    df = pd.merge(df, emarates, how='left', on='poolSymbol')
    df = df[~df.duplicated()].copy()
    df.reset_index(inplace=True, drop=True)
    df.loc[:,'reserveTokenAmount_real'] = [Decimal(x) for x in df.reserveTokenAmount_real]
    df.loc[:,'reserveTokenAmount_real_bnt'] = df.reserveTokenAmount_real * df.emaRate
    df.loc[:,'reserveTokenAmount_real_usd'] = df.reserveTokenAmount_real_bnt * df.bntprice
    df = df.astype(str)
    df.to_parquet(ETL_CSV_STORAGE_DIRECTORY+f"{stringdf}.parquet", compression='gzip')

# COMMAND ----------

def update_basic_poolData():

    # Really I just wanted the historic ema and spot rate for specific blocknumbers, however this requires a call to poolData, so you get all this info for free

    existing = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'PoolCollection_TokensDeposited_poolData.parquet')
    existing.blocknumber = existing.blocknumber.astype(int)
    max_block = existing.blocknumber.max() - 10000

    deposits_tkn = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_PoolCollection_TokensDeposited.parquet")
    deposits_tkn.blocknumber = deposits_tkn.blocknumber.astype(int)
    deposits_tkn = deposits_tkn[deposits_tkn.blocknumber>=max_block].copy()

    mdf = pd.DataFrame()
    for i in deposits_tkn.index:
        blocknumber = int(deposits_tkn.blocknumber[i])
        tkn = deposits_tkn.tokenSymbol[i]
        addy = BancorNetwork['contract'].functions.collectionByPool(tokenAddresses[tkn]).call(block_identifier = blocknumber)
        pooldata = PoolCollectionset[PoolCollectionAddys[addy]]['contract'].functions.poolData(tokenAddresses[tkn]).call(block_identifier=blocknumber)
        if addy in ["0x6f9124C32a9f6E532C908798F872d5472e9Cb714", "0xEC9596e0eB67228d61a12CfdB4b3608281F261b3",]: # V1, V2
            poolToken, tradingFeePPM, tradingEnabled, depositingEnabled, averageRate, depositLimit, tknPool_liquidity = pooldata
            bntTradingLiquidity, tknTradingLiquidity, stakingLedger_tknBalance = tknPool_liquidity
            ema_blocknum, ema_tuple = averageRate
            emaCompressedNumerator, emaCompressedDenominator = ema_tuple
            emaInvCompressedNumerator = Decimal('0')
            emaInvCompressedDenominator = Decimal('1')
        elif addy in ["0xF506B96891dDe3c149FF08b2FF26a059258f7eC7","0xAD3339099ae87f1ad6e984872B95E7be24b813A7","0xb8d8033f7B2267FEFfdBAA521Cd8a86DF861Da69","0x05E29F07B9710368A1D5658750e9B4B478c15bB8","0x395eD9ffd32b255dBD128092ABa40200159d664b", "0xD2a572fEfdbD719605334DF5CBA9746e02D51558","0x5cE51256651aA90eee24259a56529afFcf13a3d0", "0xd982e001491D414c857F2A1aaA4B43Ccf9f642B4",]: #V3, V4, V5, V6, V7, V8, V9, V10
            poolToken, tradingFeePPM, tradingEnabled, depositingEnabled, averageRate, tknPool_liquidity = pooldata
            depositLimit = Decimal('0')
            bntTradingLiquidity, tknTradingLiquidity, stakingLedger_tknBalance = tknPool_liquidity
            ema_blocknum, ema_tuple, emaInv_tuple = averageRate
            emaCompressedNumerator, emaCompressedDenominator = ema_tuple
            emaInvCompressedNumerator, emaInvCompressedDenominator = emaInv_tuple

        else:
            print('Unknown PoolCollection Addy')
            break

        emaCompressedNumerator_real = Decimal(emaCompressedNumerator) / Decimal('10')**Decimal('18')
        emaCompressedDenominator_real = Decimal(emaCompressedDenominator) / Decimal('10')**Decimal(str(tokenDecimals[tkn]))
        emaInvCompressedNumerator_real = Decimal(emaInvCompressedNumerator) / Decimal('10')**Decimal(str(tokenDecimals[tkn]))
        emaInvCompressedDenominator_real = Decimal(emaInvCompressedDenominator) / Decimal('10')**Decimal('18')

        emaRate = emaCompressedNumerator_real/emaCompressedDenominator_real
        emaInvRate = emaInvCompressedNumerator_real/emaInvCompressedDenominator_real

        spotRate = get_spot_rate(bntTradingLiquidity, tknTradingLiquidity, str(tokenDecimals[tkn]))

        emaDeviation = get_emaDeviation(emaRate, spotRate)
        emaInvDeviation = get_emaInvDeviation(emaInvRate, spotRate)

        df = pd.DataFrame([(tkn, blocknumber, tradingFeePPM, tradingEnabled, depositingEnabled, bntTradingLiquidity, tknTradingLiquidity, stakingLedger_tknBalance, ema_blocknum, emaCompressedNumerator, emaCompressedDenominator, emaInvCompressedNumerator, emaInvCompressedDenominator,
                           emaCompressedNumerator_real, emaCompressedDenominator_real, emaInvCompressedNumerator_real, emaInvCompressedDenominator_real, emaRate, emaInvRate, spotRate, emaDeviation, emaInvDeviation)],
                         columns = [
                             'poolSymbol', 'blocknumber', 'tradingFeePPM', 'tradingEnabled', 'depositingEnabled', 'bntTradingLiquidity', 'tknTradingLiquidity', 'stakedBalance', 'emaBlockNumber', 'emaCompressedNumerator', 'emaCompressedDenominator', 'emaInvCompressedNumerator', 'emaInvCompressedDenominator',
                           'emaCompressedNumerator_real', 'emaCompressedDenominator_real', 'emaInvCompressedNumerator_real', 'emaInvCompressedDenominator_real', 'emaRate', 'emaInvRate', 'spotRate', 'emaDeviation', 'emaInvDeviation'
                         ],
                         index = [tkn])
        mdf = mdf.append(df)

    mdf = mdf.astype(str)
    existing = existing.append(mdf)
    existing = existing[~existing.duplicated()].copy()
    existing = existing.astype(str)
    existing.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'PoolCollection_TokensDeposited_poolData.parquet', compression='gzip')

# COMMAND ----------

update_basic_poolData()

# COMMAND ----------

def get_v3_deposits_table():
    deposits_bnt = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_BNTPool_TokensDeposited.parquet")
    deposits_bnt.rename(columns = {'bntAmount': 'tokenAmount', 'bntAmount_real': 'tokenAmount_real'}, inplace=True)
    deposits_bnt.loc[:,'token'] = '0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C'
    deposits_bnt.loc[:,'tokenSymbol'] = 'bnt'
    deposits_bnt.loc[:,'tokenDecimals'] = '18'
    deposits_bnt.loc[:,'emaRate'] = '1'
    deposits_bnt.loc[:,'spotRate'] = '1'

    deposits_tkn = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_PoolCollection_TokensDeposited.parquet")
    sub = deposits_tkn[~deposits_tkn.baseTokenAmount.isnull()].copy()
    sub.loc[:,'tokenAmount'] = sub.loc[:,'baseTokenAmount'] 
    sub.loc[:,'tokenAmount_real'] = sub.loc[:,'baseTokenAmount_real'] 
    deposits_tkn.drop(sub.index.values, inplace=True)
    deposits_tkn = deposits_tkn.append(sub)
    deposits_tkn.drop(['baseTokenAmount','baseTokenAmount_real'], axis=1, inplace=True)
    deposits_tkn.sort_values(by='blocknumber', inplace=True)
    deposits_tkn.loc[:,'vbntAmount'] = '0'
    deposits_tkn.loc[:,'vbntAmount_real'] = '0'

    mdf = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'PoolCollection_TokensDeposited_poolData.parquet')
    deposits_tkn = pd.merge(deposits_tkn, mdf[['blocknumber','emaRate','spotRate']], how='left', left_on = ['blocknumber','tokenSymbol'], right_on=['blocknumber', mdf.index])
    deposits_tkn = deposits_tkn[~deposits_tkn.duplicated()].copy()

    deposits = deposits_tkn.append(deposits_bnt)
    deposits.sort_values(by=['blocknumber','tokenSymbol'], inplace=True)
    deposits.reset_index(inplace=True, drop=True)
    deposits = deposits.astype(str)
    deposits.to_parquet(ETL_CSV_STORAGE_DIRECTORY+"Events_All_TokensDeposited.parquet", compression='gzip')

# COMMAND ----------

get_v3_deposits_table()

# COMMAND ----------

# DBTITLE 1,Fetch v2 trades
def get_v2_trade_data():
##  !!!!  IMPORTANT READ ONLY from this database  !!!!
    engine = create_engine(ETL_BANCOR_V2_DB)
    tokensTraded_v2 = pd.read_sql_query(
        '''
        select * 
        from "conversion_step" 
        where "timestamp" >= '2022-01-01'
        ''',con=engine, dtype=str)
    tokensTraded_v2.blockId = tokensTraded_v2.blockId.astype(int)
    tokensTraded_v2.sort_values(by='blockId', inplace=True)
    
    # relabelling the old RPL token
    indexes = list(tokensTraded_v2[tokensTraded_v2.toToken == '0xB4EFd85c19999D84251304bDA99E90B92300Bd93'].index)
    tokensTraded_v2.loc[indexes,'toSymbol'] = 'RPL[old]'

    indexes2 = list(tokensTraded_v2[tokensTraded_v2.fromToken == '0xB4EFd85c19999D84251304bDA99E90B92300Bd93'].index)
    tokensTraded_v2.loc[indexes2,'fromSymbol'] = 'RPL[old]'

    blockNumber_to_timestamp = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'blockNumber_to_timestamp.parquet')
    blockNumber_to_timestamp.rename(columns = {'blockNumber':'blocknumber'}, inplace=True)
    blockNumber_to_timestamp.blocknumber = blockNumber_to_timestamp.blocknumber.astype(int)
    max_block = blockNumber_to_timestamp.blocknumber.max()

    tokensTraded_v2 = tokensTraded_v2[tokensTraded_v2.blockId<=max_block].copy()
    tokensTraded_v2.rename(columns = {
        'timestamp':'time',
        'blockId': 'blocknumber',
        'transactionHash': 'txhash',
        'fromToken': 'sourceToken',
        'toToken': 'targetToken',
        'fromSymbol': 'sourceSymbol',
        'toSymbol': 'targetSymbol',
        'inputAmount': 'sourceAmount_real',
        'outputAmount': 'targetAmount_real',
        'bnt': 'targetAmount_real_bnt',
        'usd': 'targetAmount_real_usd',
        'conversionFee': 'targetFeeAmount_real',
        'feeBnt': 'targetFeeAmount_real_bnt',
        'feeUsd': 'targetFeeAmount_real_usd',
    }, inplace=True)
    tokensTraded_v2.loc[:,'actualTotalFees_real_usd'] = tokensTraded_v2.targetFeeAmount_real_usd
    tokensTraded_v2 = tokensTraded_v2[['time', 'blocknumber', 'txhash', 'sourceToken', 'targetToken', 'sourceSymbol', 'targetSymbol', 'sourceDecimals', 'targetDecimals', 'sourceAmount_real', 'targetAmount_real', 
                     'targetAmount_real_bnt', 'targetAmount_real_usd', 'targetFeeAmount_real', 'targetFeeAmount_real_bnt', 'targetFeeAmount_real_usd', 'actualTotalFees_real_usd']].copy()

    tokensTraded_v2.sourceSymbol = [x.lower() for x in tokensTraded_v2.sourceSymbol]
    tokensTraded_v2.targetSymbol = [x.lower() for x in tokensTraded_v2.targetSymbol]
    tokensTraded_v2.time = [x[:19] for x in tokensTraded_v2.time]
    tokensTraded_v2.reset_index(inplace=True, drop=True)
    tokensTraded_v2 = tokensTraded_v2.astype(str)
    tokensTraded_v2.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'versionTwo_TokensTraded.parquet', compression='gzip')

# COMMAND ----------

get_v2_trade_data()

# COMMAND ----------

def create_v2_v3_daily_summaries():
    tokensTraded_v2_daily = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'versionTwo_TokensTraded.parquet')
    tokensTraded_v2_daily.loc[:,'day'] = [x[:10] for x in tokensTraded_v2_daily.time]
    for col in ['sourceAmount_real','targetAmount_real','targetAmount_real_bnt','targetAmount_real_usd','targetFeeAmount_real','targetFeeAmount_real_bnt','targetFeeAmount_real_usd','actualTotalFees_real_usd']:
        tokensTraded_v2_daily.loc[:,col] = tokensTraded_v2_daily.loc[:,col].apply(lambda x: Decimal(str(x)))

    uniqueSymbols = sorted(list(set(list(tokensTraded_v2_daily.sourceSymbol) + list(tokensTraded_v2_daily.targetSymbol))))
    uniqueSymbols.remove('bnt')
    small_tokensTraded_v2_daily = tokensTraded_v2_daily[['day', 'txhash', 'sourceSymbol', 'targetSymbol', 'sourceAmount_real',
           'targetAmount_real', 'targetAmount_real_bnt', 'targetAmount_real_usd']].copy()

    tokensTraded_v2_daily_volume = pd.DataFrame()
    for symbol in uniqueSymbols:
        symbolindexes = list(small_tokensTraded_v2_daily[small_tokensTraded_v2_daily.sourceSymbol==symbol].index) + list(small_tokensTraded_v2_daily[small_tokensTraded_v2_daily.targetSymbol==symbol].index)
        df = small_tokensTraded_v2_daily.iloc[symbolindexes].copy()
        df2 = df[['day', 'sourceSymbol', 'targetSymbol','txhash']].copy()

        dftradecounts = df.groupby('day')['txhash'].count().reset_index()
        dftradecounts.columns = ['day', 'v2_daily_trade_count']
        df.drop(['sourceSymbol', 'targetSymbol','sourceAmount_real','txhash'], axis=1, inplace=True)
        dfsums = df.groupby('day').sum(numeric_only=False)
        dfsums.reset_index(inplace=True)
        dfsums = pd.merge(dfsums, df2.groupby('day')['txhash'].nunique().reset_index(), how='left')
        dfsums = pd.merge(dfsums, dftradecounts, how='left')
        dfsums.rename(columns = {'txhash':'tx_count'}, inplace=True)
        dfsums.loc[:,'v2_average_trade_size_usd'] = dfsums.targetAmount_real_usd / dfsums.v2_daily_trade_count
        dfsums.loc[:,'symbol'] = symbol
        tokensTraded_v2_daily_volume = tokensTraded_v2_daily_volume.append(dfsums)

    tokensTraded_v2_daily_volume.sort_values(by = ['day', 'symbol'], inplace=True)
    tokensTraded_v2_daily_volume.rename(columns = {'symbol':'poolSymbol', 'targetAmount_real': 'v2_totalVolume_real', 'targetAmount_real_bnt': 'v2_totalVolume_real_bnt', 'targetAmount_real_usd':'v2_totalVolume_real_usd', 'tx_count':'v2_daily_tx_count'}, inplace=True)
    tokensTraded_v2_daily_volume.reset_index(inplace=True, drop=True)

    tokensTraded_v2_daily_fees = tokensTraded_v2_daily[['day', 'sourceSymbol', 'targetSymbol', 'sourceAmount_real','targetAmount_real','targetAmount_real_bnt', 'targetAmount_real_usd','targetFeeAmount_real','targetFeeAmount_real_bnt','targetFeeAmount_real_usd','actualTotalFees_real_usd']].copy()
    tokensTraded_v2_daily_fees = tokensTraded_v2_daily.groupby(['day','targetSymbol']).sum(numeric_only=False)
    tokensTraded_v2_daily_fees.drop(['sourceSymbol','sourceAmount_real', 'targetAmount_real','targetAmount_real_bnt', 'targetAmount_real_usd'], axis=1, inplace=True)
    tokensTraded_v2_daily_fees.sort_values(by = ['day', 'targetSymbol'], inplace=True)
    tokensTraded_v2_daily_fees.reset_index(inplace=True)
    tokensTraded_v2_daily_fees.rename(columns = {'targetSymbol':'poolSymbol','targetFeeAmount_real':'v2_targetFeeAmount_real', 'targetFeeAmount_real_bnt':'v2_targetFeeAmount_real_bnt','targetFeeAmount_real_usd':'v2_targetFeeAmount_real_usd', 'actualTotalFees_real_usd':'v2_actualTotalFees_real_usd'}, inplace=True)
    tokensTraded_v2_daily_fees = tokensTraded_v2_daily_fees[['day','poolSymbol', 'v2_targetFeeAmount_real', 'v2_targetFeeAmount_real_bnt', 'v2_targetFeeAmount_real_usd', 'v2_actualTotalFees_real_usd']].copy()

    v2_tokensTraded_combined = pd.merge(tokensTraded_v2_daily_volume,tokensTraded_v2_daily_fees, how='outer', on=['day','poolSymbol'])
    v2_tokensTraded_combined.fillna(Decimal('0'), inplace=True)
    v2_tokensTraded_combined.loc[:,'v2_daily_trade_count'] = [Decimal(x) for x in v2_tokensTraded_combined.v2_daily_trade_count]
    v2_tokensTraded_combined.loc[:,'v2_average_fee_size_usd'] = [get_safe_divide(v2_tokensTraded_combined.v2_actualTotalFees_real_usd[i], v2_tokensTraded_combined.v2_daily_trade_count[i]) for i in  v2_tokensTraded_combined.index]



    tokensTraded_v3_daily = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_BancorNetwork_TokensTraded_Updated.parquet')
    tokensTraded_v3_daily.loc[:,'day'] = [x[:10] for x in tokensTraded_v3_daily.time]
    for col in ['sourceAmount_real', 'sourceAmount_real_bnt', 'sourceAmount_real_usd', 'targetAmount_real','targetAmount_real_bnt', 'targetAmount_real_usd','targetFeeAmount_real','targetFeeAmount_real_bnt','targetFeeAmount_real_usd','actualTotalFees_real_usd']:
        tokensTraded_v3_daily.loc[:,col] = tokensTraded_v3_daily.loc[:,col].apply(lambda x: Decimal(str(x)))

    uniqueSymbols = sorted(list(set(list(tokensTraded_v3_daily.sourceSymbol) + list(tokensTraded_v3_daily.targetSymbol))))
    uniqueSymbols.remove('bnt')
    small_tokensTraded_v3_daily = tokensTraded_v3_daily[['day', 'txhash', 'sourceSymbol', 'targetSymbol', 'sourceAmount_real', 'sourceAmount_real_bnt', 'sourceAmount_real_usd',
           'targetAmount_real', 'targetAmount_real_bnt', 'targetAmount_real_usd']].copy()

    tokensTraded_v3_daily_volume = pd.DataFrame()
    for symbol in uniqueSymbols:
        df = small_tokensTraded_v3_daily[small_tokensTraded_v3_daily.targetSymbol==symbol].copy()
        dfcounts = df.groupby('day')['txhash'].nunique().reset_index()
        dftradecounts = df.groupby('day')['txhash'].count().reset_index()
        dftradecounts.columns = ['day', 'v3_daily_trade_count']
        df.drop('txhash', axis=1, inplace=True)
        dfsums = df.groupby('day').sum(numeric_only=False)

        dfsums.reset_index(inplace=True)
        dfsums.loc[:,'v3_totalVolume_real'] = dfsums.sourceAmount_real + dfsums.targetAmount_real
        dfsums.loc[:,'v3_totalVolume_real_bnt'] = dfsums.sourceAmount_real_bnt + dfsums.targetAmount_real_bnt
        dfsums.loc[:,'v3_totalVolume_real_usd'] = dfsums.sourceAmount_real_usd + dfsums.targetAmount_real_usd
        dfsums = pd.merge(dfsums, dfcounts, how='left')
        dfsums = pd.merge(dfsums, dftradecounts, how='left')
        dfsums.rename(columns = {'txhash':'v3_daily_tx_count'}, inplace=True)
        dfsums.loc[:,'v3_average_trade_size_usd'] = dfsums.targetAmount_real_usd / dfsums.v3_daily_trade_count
        dfsums.drop(['sourceSymbol', 'targetSymbol', 'sourceAmount_real','sourceAmount_real_bnt','sourceAmount_real_usd','targetAmount_real','targetAmount_real_bnt','targetAmount_real_usd'], axis=1, inplace=True)
        if len(dfsums)>0:
            dfsums.loc[:,'symbol'] = symbol
            tokensTraded_v3_daily_volume = tokensTraded_v3_daily_volume.append(dfsums)
        else:
            pass

    tokensTraded_v3_daily_volume.rename(columns = {'symbol':'poolSymbol'}, inplace=True)

    tokensTraded_v3_daily_fees = tokensTraded_v3_daily[['day', 'sourceSymbol', 'targetSymbol', 'sourceAmount_real', 'sourceAmount_real_bnt', 'sourceAmount_real_usd','targetAmount_real','targetAmount_real_bnt', 'targetAmount_real_usd','targetFeeAmount_real','targetFeeAmount_real_bnt','targetFeeAmount_real_usd','actualTotalFees_real_usd']].copy()
    tokensTraded_v3_daily_fees = tokensTraded_v3_daily.groupby(['day','targetSymbol']).sum(numeric_only=False)
    tokensTraded_v3_daily_fees.drop(['sourceSymbol','sourceAmount_real', 'sourceAmount_real_bnt', 'sourceAmount_real_usd', 'targetAmount_real','targetAmount_real_bnt', 'targetAmount_real_usd'], axis=1, inplace=True)
    tokensTraded_v3_daily_fees.sort_values(by = ['day', 'targetSymbol'], inplace=True)
    tokensTraded_v3_daily_fees.reset_index(inplace=True)
    tokensTraded_v3_daily_fees.rename(columns = {'targetSymbol':'poolSymbol','targetFeeAmount_real':'v3_targetFeeAmount_real', 'targetFeeAmount_real_bnt':'v3_targetFeeAmount_real_bnt','targetFeeAmount_real_usd':'v3_targetFeeAmount_real_usd', 'actualTotalFees_real_usd':'v3_actualTotalFees_real_usd'}, inplace=True)
    tokensTraded_v3_daily_fees = tokensTraded_v3_daily_fees[['day','poolSymbol', 'v3_targetFeeAmount_real', 'v3_targetFeeAmount_real_bnt', 'v3_targetFeeAmount_real_usd', 'v3_actualTotalFees_real_usd']].copy()

    v3_tokensTraded_combined = pd.merge(tokensTraded_v3_daily_volume,tokensTraded_v3_daily_fees, how='outer', on=['day','poolSymbol'])
    v3_tokensTraded_combined.fillna(Decimal('0'), inplace=True)
    v3_tokensTraded_combined.loc[:,'v3_daily_trade_count'] = [Decimal(x) for x in v3_tokensTraded_combined.v3_daily_trade_count]
    v3_tokensTraded_combined.loc[:,'v3_average_fee_size_usd'] = [get_safe_divide(v3_tokensTraded_combined.v3_actualTotalFees_real_usd[i], v3_tokensTraded_combined.v3_daily_trade_count[i]) for i in  v3_tokensTraded_combined.index]


    tokensTraded_combined = pd.merge(v2_tokensTraded_combined,v3_tokensTraded_combined, how='outer', on=['day','poolSymbol'])
    tokensTraded_combined.fillna(Decimal('0'), inplace=True)
    tokensTraded_combined.rename(columns = {'day': 'time'}, inplace=True)
    tokensTraded_combined = tokensTraded_combined.astype(str)
    tokensTraded_combined.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_combined_TokensTraded_daily.parquet', compression='gzip')

# COMMAND ----------

create_v2_v3_daily_summaries()

# COMMAND ----------

def get_slippage_stats():
    tl = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_PoolCollection_TradingLiquidityUpdated.parquet')
    Events_BancorNetwork_TokensTraded_Updated = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_BancorNetwork_TokensTraded_Updated.parquet')
    mindf = pd.merge(Events_BancorNetwork_TokensTraded_Updated, tl[['contextId', 'tokenSymbol', 'prevLiquidity_real']], how='left', left_on = ['contextId', 'sourceSymbol'], right_on=['contextId', 'tokenSymbol'])
    mindf = mindf[['time','sourceSymbol', 'sourceAmount_real', 'prevLiquidity_real']].copy()
    mindf.loc[:,'sourceAmount_real'] = [Decimal(x) for x in mindf.sourceAmount_real]
    mindf.loc[:,'prevLiquidity_real'] = [Decimal(x) for x in mindf.prevLiquidity_real]
    mindf.loc[:,'priceImpact_perc'] =  (((mindf.sourceAmount_real/mindf.prevLiquidity_real) + 1)**2 - 1)
    mindf.loc[:,'slippage_perc'] =  mindf.sourceAmount_real / (mindf.prevLiquidity_real + mindf.sourceAmount_real)
    mindf = mindf.astype(str)
    mindf.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_Trade_Slippage_Stats.parquet', compression='gzip')

# COMMAND ----------

get_slippage_stats()

# COMMAND ----------

# DBTITLE 1,BNT/VBNT Supply Summary
def get_bntvbnt_tokensupply():
    # vbnt
    vbnt_raw_issuance = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'Events_VBNT_Issuance.parquet')
    vbnt_raw_issuance.loc[:,'amount_real'] = [Decimal(x) for x in vbnt_raw_issuance.amount_real]
    vbnt_raw_issuance.loc[:,'day'] = [str(x)[:10] for x in vbnt_raw_issuance.time]
    daily_vbnt_issuance = vbnt_raw_issuance[['day','amount_real']].groupby('day').sum(numeric_only=False).reset_index()
    daily_vbnt_issuance.columns = ['day', 'vbnt_issuance']

    vbnt_raw_destruction = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'Events_VBNT_Destruction.parquet')
    vbnt_raw_destruction.loc[:,'amount_real'] = [Decimal(x) for x in vbnt_raw_destruction.amount_real]
    vbnt_raw_destruction.loc[:,'day'] = [str(x)[:10] for x in vbnt_raw_destruction.time]
    daily_vbnt_destruction = vbnt_raw_destruction[['day','amount_real']].groupby('day').sum(numeric_only=False).reset_index()
    daily_vbnt_destruction.columns = ['day', 'vbnt_destruction']

    daily_vbnt = pd.merge(daily_vbnt_issuance, daily_vbnt_destruction, how='outer')
    daily_vbnt.fillna(Decimal('0'), inplace=True)
    daily_vbnt.sort_values(by='day', inplace=True)
    daily_vbnt.loc[:,'daily_vbnt_net'] = daily_vbnt.vbnt_issuance - daily_vbnt.vbnt_destruction
    daily_vbnt.loc[:,'vbnt_supply'] = daily_vbnt.daily_vbnt_net.cumsum()
    
    #bnt
    bnt_raw_issuance = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'Events_BNT_Issuance.parquet')
    bnt_raw_issuance.loc[:,'amount_real'] = [Decimal(x) for x in bnt_raw_issuance.amount_real]
    bnt_raw_issuance.loc[:,'day'] = [str(x)[:10] for x in bnt_raw_issuance.time]
    daily_bnt_issuance = bnt_raw_issuance[['day','amount_real']].groupby('day').sum(numeric_only=False).reset_index()
    daily_bnt_issuance.columns = ['day', 'bnt_issuance']

    bnt_raw_destruction = pd.read_parquet(ETL_CSV_STORAGE_DIRECTORY+f'Events_BNT_Destruction.parquet')
    bnt_raw_destruction.loc[:,'amount_real'] = [Decimal(x) for x in bnt_raw_destruction.amount_real]
    bnt_raw_destruction.loc[:,'day'] = [str(x)[:10] for x in bnt_raw_destruction.time]
    daily_bnt_destruction = bnt_raw_destruction[['day','amount_real']].groupby('day').sum(numeric_only=False).reset_index()
    daily_bnt_destruction.columns = ['day', 'bnt_destruction']

    daily_bnt = pd.merge(daily_bnt_issuance, daily_bnt_destruction, how='outer')
    daily_bnt.fillna(Decimal('0'), inplace=True)
    daily_bnt.sort_values(by='day', inplace=True)
    daily_bnt.loc[:,'daily_bnt_net'] = daily_bnt.bnt_issuance - daily_bnt.bnt_destruction
    daily_bnt.loc[:,'bnt_supply'] = daily_bnt.daily_bnt_net.cumsum()
    
    #combine
    dates = pd.DataFrame(pd.date_range(start="2017-06-10",end=str(datetime.datetime.now())[:10], freq='D'), columns = ['day'])
    dates = dates.astype(str)
    bnt_vbnt_supply = pd.merge(dates, daily_bnt, how='left', on='day')
    bnt_vbnt_supply = pd.merge(bnt_vbnt_supply, daily_vbnt, how='left', on='day')
    bnt_vbnt_supply.fillna(Decimal('0'), inplace=True)
    bnt_vbnt_supply = bnt_vbnt_supply.astype(str)
    bnt_vbnt_supply.rename(columns = {'day': 'time'}, inplace=True)
    bnt_vbnt_supply.to_parquet(ETL_CSV_STORAGE_DIRECTORY+'Events_BNT_VBNT_DailyTokenSupply.parquet', compression='gzip')

# COMMAND ----------

get_bntvbnt_tokensupply()

# COMMAND ----------

eventsfiles = glob.glob(ETL_CSV_STORAGE_DIRECTORY+'Events_**')
eventsfiles = [x for x in eventsfiles if 'parquet' in x]
print("Events Files:",len(eventsfiles))
cols = []
for file in eventsfiles:
    col = pd.read_parquet(file).columns.tolist()
    cols += col
setcols = sorted(list(set(cols)))
print("Columns:", len(setcols))
# print(setcols)

# COMMAND ----------

data_dictionary = pd.read_csv('/dbfs/FileStore/tables/onchain_events/data_dictionary.csv')
missing_from_datadict = set(setcols) - set(sorted(list(set(data_dictionary.Column))))
print(len(missing_from_datadict))
print(missing_from_datadict)

# COMMAND ----------

set(sorted(list(set(data_dictionary.Column)))) - set(setcols)

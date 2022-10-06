# %%
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

# %%
from web3 import Web3
import os
import json
from decimal import Decimal
from bancor_etl.constants import *

# %%
url = f'https://eth-mainnet.alchemyapi.io/v2/{ETL_ALCHEMY_APIKEY}'

# %%
# HTTPProvider:
w3 = Web3(Web3.HTTPProvider(url))
w3.isConnected()

# %%
erc20_abi = '[{"inputs":[{"internalType":"uint256","name":"chainId_","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"guy","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":true,"inputs":[{"indexed":true,"internalType":"bytes4","name":"sig","type":"bytes4"},{"indexed":true,"internalType":"address","name":"usr","type":"address"},{"indexed":true,"internalType":"bytes32","name":"arg1","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg2","type":"bytes32"},{"indexed":false,"internalType":"bytes","name":"data","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"dst","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"burn","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"deny","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"holder","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"bool","name":"allowed","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"pull","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"push","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"rely","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"wards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'

v2poolToken_abi = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_disable","type":"bool"}],"name":"disableTransfers","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"standard","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_token","type":"address"},{"name":"_to","type":"address"},{"name":"_amount","type":"uint256"}],"name":"withdrawTokens","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"acceptOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_amount","type":"uint256"}],"name":"issue","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_amount","type":"uint256"}],"name":"destroy","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"transfersEnabled","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"newOwner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[{"name":"_name","type":"string"},{"name":"_symbol","type":"string"},{"name":"_decimals","type":"uint8"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_token","type":"address"}],"name":"NewSmartToken","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_amount","type":"uint256"}],"name":"Issuance","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_amount","type":"uint256"}],"name":"Destruction","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_spender","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_prevOwner","type":"address"},{"indexed":true,"name":"_newOwner","type":"address"}],"name":"OwnerUpdate","type":"event"}]'

converter_abi = '[{"inputs":[{"internalType":"contract IConverterAnchor","name":"_anchor","type":"address"},{"internalType":"contract IContractRegistry","name":"_registry","type":"address"},{"internalType":"uint32","name":"_maxConversionFee","type":"uint32"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint16","name":"_type","type":"uint16"},{"indexed":true,"internalType":"contract IConverterAnchor","name":"_anchor","type":"address"},{"indexed":true,"internalType":"bool","name":"_activated","type":"bool"}],"name":"Activation","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"contract IERC20Token","name":"_fromToken","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_toToken","type":"address"},{"indexed":true,"internalType":"address","name":"_trader","type":"address"},{"indexed":false,"internalType":"uint256","name":"_amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_return","type":"uint256"},{"indexed":false,"internalType":"int256","name":"_conversionFee","type":"int256"}],"name":"Conversion","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint32","name":"_prevFee","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"_newFee","type":"uint32"}],"name":"ConversionFeeUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"_provider","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"_amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newBalance","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newSupply","type":"uint256"}],"name":"LiquidityAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"_provider","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"_amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newBalance","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_newSupply","type":"uint256"}],"name":"LiquidityRemoved","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"_prevOwner","type":"address"},{"indexed":true,"internalType":"address","name":"_newOwner","type":"address"}],"name":"OwnerUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"contract IERC20Token","name":"_token1","type":"address"},{"indexed":true,"internalType":"contract IERC20Token","name":"_token2","type":"address"},{"indexed":false,"internalType":"uint256","name":"_rateN","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_rateD","type":"uint256"}],"name":"TokenRateUpdate","type":"event"},{"inputs":[],"name":"acceptAnchorOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"acceptOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"acceptTokenOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_reserve1Amount","type":"uint256"},{"internalType":"uint256","name":"_reserve2Amount","type":"uint256"},{"internalType":"uint256","name":"_minReturn","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"},{"internalType":"uint256[]","name":"_reserveAmounts","type":"uint256[]"},{"internalType":"uint256","name":"_minReturn","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMu|tability":"payable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"},{"internalType":"uint256","name":"_reserveTokenIndex","type":"uint256"},{"internalType":"uint256","name":"_reserveAmount","type":"uint256"}],"name":"addLiquidityCost","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"},{"internalType":"uint256","name":"_reserveAmount","type":"uint256"}],"name":"addLiquidityReturn","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_token","type":"address"},{"internalType":"uint32","name":"_weight","type":"uint32"}],"name":"addReserve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"anchor","outputs":[{"internalType":"contract IConverterAnchor","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"averageRateInfo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"connectorTokenCount","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_index","type":"uint256"}],"name":"connectorTokens","outputs":[{"internalType":"contract IERC20Token","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_address","type":"address"}],"name":"connectors","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint32","name":"","type":"uint32"},{"internalType":"bool","name":"","type":"bool"},{"internalType":"bool","name":"","type":"bool"},{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"conversionFee","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_sourceToken","type":"address"},{"internalType":"contract IERC20Token","name":"_targetToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"address","name":"_trader","type":"address"},{"internalType":"address payable","name":"_beneficiary","type":"address"}],"name":"convert","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"converterType","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_connectorToken","type":"address"}],"name":"getConnectorBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_sourceToken","type":"address"},{"internalType":"contract IERC20Token","name":"_targetToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"getReturn","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isV28OrHigher","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"maxConversionFee","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"newOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"onlyOwnerCanUpdateRegistry","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"prevRegistry","outputs":[{"internalType":"contract IContractRegistry","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_token","type":"address"}],"name":"recentAverageRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"registry","outputs":[{"internalType":"contract IContractRegistry","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_reserve1MinReturn","type":"uint256"},{"internalType":"uint256","name":"_reserve2MinReturn","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"},{"internalType":"uint256[]","name":"_reserveMinReturnAmounts","type":"uint256[]"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"contract IERC20Token[]","name":"_reserveTokens","type":"address[]"}],"name":"removeLiquidityReturn","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"}],"name":"reserveBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reserveBalances","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reserveTokenCount","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"reserveTokens","outputs":[{"internalType":"contract IERC20Token[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_reserveToken","type":"address"}],"name":"reserveWeight","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"restoreRegistry","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_onlyOwnerCanUpdateRegistry","type":"bool"}],"name":"restrictRegistryUpdate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint32","name":"_conversionFee","type":"uint32"}],"name":"setConversionFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_sourceToken","type":"address"},{"internalType":"contract IERC20Token","name":"_targetToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"targetAmountAndFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token","outputs":[{"internalType":"contract IConverterAnchor","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_newOwner","type":"address"}],"name":"transferAnchorOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newOwner","type":"address"}],"name":"transferTokenOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"updateRegistry","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"upgrade","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"uint16","name":"","type":"uint16"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address payable","name":"_to","type":"address"}],"name":"withdrawETH","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IERC20Token","name":"_token","type":"address"},{"internalType":"address","name":"_to","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"withdrawTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]'

# %%
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

# %%
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

# %%
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

# %%
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

# v2 contracts
CheckpointStore = load_contract('v2/CheckpointStore')
ContractRegistry = load_contract('v2/ContractRegistry')
LegacyBancorNetwork = load_contract('v2/LegacyBancorNetwork')
LegacyLiquidityProtection = load_contract('v2/LegacyLiquidityProtection')
LegacyLiquidityProtection2 = load_contract('v2/LegacyLiquidityProtection2')
LegacyLiquidityProtection3 = load_contract('v2/LegacyLiquidityProtection3')
LiquidityProtectionSettings = load_contract('v2/LiquidityProtectionSettings')
LiquidityProtectionStats = load_contract('v2/LiquidityProtectionStats')
LiquidityProtectionStore = load_contract('v2/LiquidityProtectionStore')
LiquidityProtectionSystemStore = load_contract('v2/LiquidityProtectionSystemStore')
LiquidityProtectionWallet = load_contract('v2/LiquidityProtectionWallet')
StakingRewards = load_contract('v2/StakingRewards')
StakingRewardsStore = load_contract('v2/StakingRewardsStore')

# %%
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


# %%
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

# %%




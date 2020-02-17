#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 14:28:06 2019

@author: devonstone

This script is for generating the features required for the alt scorecard

"""
###### modules needed ######
import pandas as pd
import urllib
from pymongo import MongoClient
import psycopg2
import re
from datetime import timedelta
import numpy as np

########################### connect to the various DB's required for the scorecard ###########################

# paylater ds db
mongoDB_user = urllib.parse.quote_plus("datalab_user")
mongoDB_password = urllib.parse.quote_plus("Bqredc!uH@MJL4#8@!TDcU?F")
connection = MongoClient('mongodb://'+mongoDB_user+':'+mongoDB_password+'@ds139646-a0.mlab.com:39646/paylaterds')
paylater_db = connection.paylaterds
#print(paylater_db.list_collection_names())

# sdk db
sdk_connection = MongoClient("mongodb://10.154.0.8:27017")
sdk_db = sdk_connection.paylater_data
#print(sdk_db.list_collection_names())

# postgres db
conn = psycopg2.connect("dbname='postgres' user='bitnami' host='10.154.0.18' password='3fabf5f663'")

########################### app feature functions ###########################

# gets apps installed for the instalation ID before application date
def get_application_list(client_id, application_date):

    app_list = []

    try:

        for c in sdk_db.client_info.find({'mambu_client_id': str(client_id).zfill(9)}
        ):

            for n in sdk_db.installed_apps.find({'registered_installation_id': c.get('registered_installation_id'),
                                                 'sent':{'$gte': application_date + timedelta(days=-90),'$lte': application_date}
            }):

                package_name = n.get('package_name')
                #extracted_name = re.search('\.([a-z]*)', package_name).group(1)
                if package_name not in app_list:
                    app_list.append(package_name)

    except:
        print("error occured")

    return app_list

# creates app features
def get_application_features(client_id, application_date):

    # variables
    app_count = None
    competitor_count = None
    banking_count = None
    news_count = None
    gambling_count = None
    vpn_count = None

    # get apps for installation_id
    apps = get_application_list(client_id, application_date)

    # if apps were extracted
    if len(apps) > 0:

        competitor_reg = re.compile('branch|aella|transsnet|palmcredit|lidya|renmoney|quickcheck')
        competitor_list =  list(filter(competitor_reg.search, apps))
        competitor_count = len(competitor_list)

        banking_reg = re.compile('bank|financ')
        banking_list = list(filter(banking_reg.search, apps))
        banking_count = len(banking_list)

        news_reg = re.compile('cnn|bbc|wsj')
        news_list = list(filter(news_reg.search, apps))
        news_count = len(news_list)

        gambling_reg = re.compile('gambling|betting|sportybet|touchlinebet|betpredictor|forebet|betsport|sportbet|betnaij|odds')
        gambling_list = list(filter(gambling_reg.search, apps))
        gambling_count = len(gambling_list)

        vpn_reg = re.compile('vpn')
        vpn_list = list(filter(vpn_reg.search, apps))
        vpn_count = len(vpn_list)

        app_count = len(apps)

    return competitor_count, banking_count, news_count, gambling_count, vpn_count, app_count


########################### device functions ###########################

# get device price from Postrgres
def get_device_price(device_name):

    if device_name:
        cur = conn.cursor()
        query = "select jumia_price, kara_price, afrimarket_price from device_prices where name = %s limit 1"
        cur.execute(query, (device_name,))
        price = cur.fetchall()
        cur.close()

        if price:
            if price[0][2]:
                return price[0][2]
            else:
                if price[0][0] == 0 and price[0][1] > 0:
                    return price[0][1]
                elif price[0][0] > 0 and price[0][1] == 0:
                    return price[0][0]
                else:
                    return np.mean([price[0][0], price[0][1]])

# categorises devices
def get_device_brand(device_name):

    if device_name:
        if re.search('tecno', device_name.lower()):
            return 'tecno'
        elif re.search('infinix', device_name.lower()):
            return 'infinix'
        elif re.search('gionee', device_name.lower()):
            return 'gionee'
        elif re.search('itel', device_name.lower()):
            return 'itel'
        elif re.search('samsung', device_name.lower()):
            return 'samsung'
        elif re.search('htc', device_name.lower()):
            return 'htc'
        elif re.search('nokia', device_name.lower()):
            return 'nokia'
        else:
            return 'other'

########################### employer functions ###########################

# get sector from employer name
def get_employer_sector(employer_name):

    if employer_name:
        employer_name = str(employer_name)
        if re.search('business|trad|buying|selling|hydro|kaduna|bida|chanchaga|club|entrepre',employer_name.lower().partition(' ')[0]):
            return 'flagged_responses'
        elif re.match('student', employer_name.lower()):
            return 'student'
        elif re.search('self', employer_name.lower()):
            return 'self_employed'
        elif re.search('army|air force|airforce|military|navy|defence|nscdc|corps|nysc|soldier', employer_name.lower()):
            return 'military'
        elif re.search('bank|finance|stanbic|insurance|standard chart|fcmb|renmoney|branch|mortgage|broker|accounting', employer_name.lower()):
            return 'financial_services'
        elif re.search('nestle|tobacco|goods|cloth|food|brewe|bevera|catering|unilever|fashion|cosmetics|wholesale|dealer|jumia|shoe', employer_name.lower()):
            return 'consumer_goods'
        elif re.search('market|advert|consult|hospitali|hotel|media|logist|dhl|courier|delivery|hair|saloon|tailo', employer_name.lower()):
            return 'client_services'
        elif re.search('poultry|farm|fish|agricul', employer_name.lower()):
            return 'agriculture'
        elif re.search('construct|building|contractor|engineer|architect', employer_name.lower()):
            return 'construction'
        elif re.search('propert|real est', employer_name.lower()):
            return 'property'
        elif (re.search('ministry|federal|government|state|immigration', employer_name.lower()) == True and re.search('bida|chanchaga|"government"', employer_name.lower()) == False):
            return 'government'
        elif re.search('civil|teach|school|police|fire|subeb|npf|health|medical|public|servant|nurs|university|education|hospital', employer_name.lower()):
            return 'public_sector'
        elif re.search('mobile|mtn|airtel|globacom|telec', employer_name.lower()):
            return 'telecommunications'
        elif re.search('uber|taxify|transport|driver|rail|road|drivin', employer_name.lower()):
            return 'transport'
        elif re.search('computer|techno| it |web |software|processing', employer_name.lower()):
            return 'it_services'
        elif re.search('mining|forest|chemicals|oil|gas|metal|stone|coal|petro', employer_name.lower()):
            return 'natural_resources'
        elif re.search('water|electr|power|sewa|garaba|waste|ibedc', employer_name.lower()):
            return 'utilities'
        else:
            return 'other'

########################### date functions ###########################

# get the application time of day
def get_application_time(application_date):

    if application_date:
        if application_date.hour < 12 and application_date.hour >= 5:
            return "morning"
        elif application_date.hour < 17 and application_date.hour >= 12:
            return "afternoon"
        elif application_date.hour < 22 and application_date.hour >= 17:
            return "evening"
        else:
            return "night"

# get the application time of day
def get_application_week(application_date):

    if application_date:
        if application_date.day < 8:
            return "weeek_1"
        elif application_date.day <15:
            return "weeek_2"
        elif application_date.day <22:
            return "weeek_3"
        else:
            return "weeek_4"

########################### sms functions ###########################
# regex strings for banks and competitors
comp_regex_string = 'renmoney|quickteller|kwikcash|kwikmoney|palmcredit|interswitch|branch|aellacredit'
bank_regex_string = 'zenith|stanbicibtc|gtbank|firstbank|fcmb|ecobank|diamond|accessbank|bank|first|uba|union|skye|fidelity|sterling|wema|keystone'

def get_sms_features(client_id, application_date):

    # sms features that are lists
    list_features = dict.fromkeys(["bank_sms_bodies", "banks", "balances",
                                   "credit_amounts", "debit_amounts", "competitor_sms_bodies",
                                   "competitors_contacted", "succesful_amounts", "unsuccesful_amounts",
                                   "loan_amounts", "loan_senders"]
                                   ,None
    )

    # sms features that are numeric values
    numeric_features = dict.fromkeys(["credit_transactions", "debit_transactions" , "insufficient_funds",
                                      "total_bank_sms", "succesful_payments", "unsuccesful_payments",
                                      "rejected_loans", "total_competitor_sms"]
                                      ,None
    )

    try:

        for c in sdk_db.client_info.find({'mambu_client_id': str(client_id).zfill(9)}
        ):


            for n in sdk_db.sms.find({'registered_installation_id': c.get('registered_installation_id'),
                              'created':{'$gte': application_date + timedelta(days=-90), '$lte': application_date}
                             }):

                # if an sms can be found for the client  then instantiate the variables
                for key in list_features:
                    if list_features[key] == None:
                        list_features[key] = []

                for key in numeric_features:
                    if numeric_features[key] == None:
                        numeric_features[key] = 0

                # check if the sms was sent from a bank
                if re.search(bank_regex_string, n.get('address'), re.IGNORECASE):
                
                    numeric_features['total_bank_sms'] += 1

                    # strip address down to identify unique address for each bank
                    bank = n.get('address').lower()
                    bank = re.split(' |-|\.', bank)
                    bank = bank[0]
                    bank = bank.replace('bank','').replace('rvsl','').replace('trvls','').replace('sms','').replace('mobile','').replace('pen','').replace('reg','')
                    # get banks that a client receieved a message from
                    if bank not in list_features['banks']:
                        list_features['banks'].append(bank)

                    # gather all bank sms for an applicant and replace bank messages
                    message = n.get('message')
                    message = message.replace("\r\n", " ")
                    message = message.replace("\n", " ")
                    list_features['bank_sms_bodies'].append(message)

                    # credit  search
                    credit = re.search('credit|cr ', n.get('message'), re.IGNORECASE)
                    if credit:
                        numeric_features['credit_transactions'] += 1
                        amount = re.search("[+-]?[0-9]{1,3}(?:,?[0-9]{3})*(?:\.[0-9]{2})", n.get('message'), re.IGNORECASE)
                        if amount:
                            amount = amount.group()
                            amount = amount.replace(',', '')
                            list_features['credit_amounts'].append(float(amount))

                    # debit  search
                    debit = re.search('debit|dr', n.get('message'), re.IGNORECASE)
                    if debit:
                        numeric_features['debit_transactions'] += 1
                        amount = re.search("[+-]?[0-9]{1,3}(?:,?[0-9]{3})*(?:\.[0-9]{2})", n.get('message'), re.IGNORECASE)
                        if amount:
                            amount = amount.group()
                            amount = amount.replace(',', '')
                            list_features['debit_amounts'].append(float(amount))

                    # balance search
                    balance = re.search('(?<=bal).*', n.get('message'), re.IGNORECASE)
                    if balance:
                        balance = balance.group()
                        amount = re.search("[+-]?[0-9]{1,3}(?:,?[0-9]{3})*(?:\.[0-9]{2})", balance, re.IGNORECASE)
                        if amount:
                            amount = amount.group()
                            amount = amount.replace(',', '')
                            list_features['balances'].append(float(amount))

                    # insufficient search
                    insufficient = re.search('insufficient', n.get('message'), re.IGNORECASE)
                    if insufficient:
                        numeric_features['insufficient_funds'] += 1


                # check if the sms was sent from a competitor
                if re.search(comp_regex_string, n.get('address'), re.IGNORECASE):

                    numeric_features['total_competitor_sms'] += 1

                    # strip address down to identify unique address for each competitor
                    competitor =  n.get('address').lower()
                    competitor = re.split(' |-|\.', competitor)
                    competitor = competitor[0]
                    competitor = competitor.replace('co','').replace('intl','').replace('mfb','').replace('ng','')
                    # get competitors that a client receieved a message from
                    if competitor not in list_features['competitors_contacted']:
                        list_features['competitors_contacted'].append(competitor)

                    # gather all competitor sms for an applicant and replace messages
                    if n.get('message'):
                        message = n.get('message')
                        message = message.replace("\r\n", " ")
                        message = message.replace("\n", " ")
                        list_features['competitor_sms_bodies'].append(message)

                    # unsuccessful payments search
                    unsuccess = re.search('unsuccessful|overdue|insufficient|missed', n.get('message'))
                    if unsuccess:
                        numeric_features['unsuccesful_payments'] += 1
                        currency = re.search('(?<=N).*', n.get('message'))
                        if currency:
                            currency = currency.group()
                            amount = re.search("[+-]?[0-9]{1,3}(?:,?[0-9]{3})*(?:\.[0-9]{2})?", currency, re.IGNORECASE)
                            if amount:
                                amount = amount.group()
                                amount = amount.replace(',', '')
                                list_features['unsuccesful_amounts'].append(float(amount))

                    # successful payments search
                    success = re.search('(?<!un)successful|repaid|paid in full', n.get('message'))
                    if success:
                        numeric_features['succesful_payments'] += 1
                        currency = re.search('(?<=N).*', n.get('message'))
                        if currency:
                            currency = currency.group()
                            amount = re.search("[+-]?[0-9]{1,3}(?:,?[0-9]{3})*(?:\.[0-9]{2})?", currency, re.IGNORECASE)
                            if amount:
                                amount = amount.group()
                                amount = amount.replace(',', '')
                                list_features['succesful_amounts'].append(float(amount))

                    # rejected loans search
                    rejected = re.search('reject', n.get('message'))
                    if rejected:
                        numeric_features['rejected_loans'] += 1

                    # loan amount  search
                    loan = re.search('sent|deposited|sending|loan balance', n.get('message'))
                    if loan:
                        currency = re.search('(?<=N).*', n.get('message'))
                        if currency:
                            currency = currency.group()
                            amount = re.search("[+-]?[0-9]{1,3}(?:,?[0-9]{3})*(?:\.[0-9]{2})?", currency, re.IGNORECASE)
                            if amount:
                                amount = amount.group()
                                amount = amount.replace(',', '')
                                list_features['loan_amounts'].append(float(amount))
                                if competitor not in list_features['loan_senders']:
                                    list_features['loan_senders'].append(competitor)

    except:
        print("error occured")

    return list_features, numeric_features

########################### bring in ids related to succesful applications ###########################

applications = pd.read_csv('applications.csv')
applications['application_date'] = pd.to_datetime(applications['application_date'])
applications = applications.where((pd.notnull(applications)), None)
########################### gather data for each application ###########################

alternative_data = {}

count = 1

# loop through each application and get the apps associated with it
for index, row in applications.iterrows():

    # check if qpplicaition id exiists
    if row['application_id']:
        alternative_data[row['application_id']] = {}

        # get app features for application
        app_feats = get_application_features(row['client_id'], row['application_date'])
        alternative_data[row['application_id']]['competitor_count'] = app_feats[0]
        alternative_data[row['application_id']]['banking_count'] = app_feats[1]
        alternative_data[row['application_id']]['news_count'] = app_feats[2]
        alternative_data[row['application_id']]['gambling_count'] = app_feats[3]
        alternative_data[row['application_id']]['vpn_count'] = app_feats[4]
        alternative_data[row['application_id']]['app_count'] = app_feats[5]

        # get device price
        alternative_data[row['application_id']]['device_price'] = get_device_price(row['device'])
        # get device brand
        alternative_data[row['application_id']]['device_brand'] = get_device_brand(row['device'])

        # get sector
        alternative_data[row['application_id']]['sector'] = get_employer_sector(row['employer_name'])

        # date features
        alternative_data[row['application_id']]['application_time'] = get_application_time(row['application_date'])
        alternative_data[row['application_id']]['application_week'] = get_application_week(row['application_date'])

#        # sms feautres
#        list_feats, numeric_feats = get_sms_features(row['client_id'] ,row['application_date'])
#
#        # bank features
#        alternative_data[row['application_id']]['banks_contacted'] =  list_feats['banks']
#
#        # sms balances
#        balances = list_feats['balances']
#        max_balance = None
#        min_balance = None
#        if balances:
#            if len(balances) > 0:
#                max_balance = max(balances)
#                min_balance = min(balances)
#            else:
#                max_balance = 0
#                min_balance = 0
#
#        alternative_data[row['application_id']]['max_balance'] =  max_balance
#        alternative_data[row['application_id']]['min_balance'] =  min_balance
#
#        # credits and debits
#        alternative_data[row['application_id']]['credit_transactions'] =  numeric_feats['credit_transactions']
#
#        credit_amounts = list_feats['credit_amounts']
#        max_creds = None
#        min_creds = None
#        if credit_amounts:
#            if len(credit_amounts) > 0:
#                max_creds = max(credit_amounts)
#                min_creds = min(credit_amounts)
#            else:
#                max_creds = 0
#                min_creds = 0
#
#        alternative_data[row['application_id']]['max_credit'] =  max_creds
#        alternative_data[row['application_id']]['min_credit'] =  min_creds
#
#        alternative_data[row['application_id']]['debit_transactions'] =  numeric_feats['debit_transactions']
#
#        debit_amounts = list_feats['debit_amounts']
#        max_debs = None
#        min_debs = None
#        if debit_amounts:
#            if len(debit_amounts):
#                max_debs = max(debit_amounts)
#                min_debs = min(debit_amounts)
#            else:
#                max_debs = 0
#                min_debs = 0
#
#        alternative_data[row['application_id']]['max_debit'] =  max_debs
#        alternative_data[row['application_id']]['min_debit'] =  min_debs
#
#        # insufficent funds
#        alternative_data[row['application_id']]['insufficient_funds'] =  numeric_feats['insufficient_funds']
#
#        # competitor features
#        alternative_data[row['application_id']]['competitors_contacted'] =  list_feats['competitors_contacted']
#
#        # successful and unsuccessful loans
#        alternative_data[row['application_id']]['succesful_payments'] =  numeric_feats['succesful_payments']
#
#        succesful_amounts = list_feats['succesful_amounts']
#        max_succ = None
#        min_succ = None
#        if succesful_amounts:
#            if len(succesful_amounts) > 0:
#                max_succ = max(succesful_amounts)
#                min_succ = min(succesful_amounts)
#            else:
#                max_succ = 0
#                min_succ = 0
#
#        alternative_data[row['application_id']]['max_succesful_loan_payment'] =  max_succ
#        alternative_data[row['application_id']]['min_succesful_loan_payment'] =  min_succ
#
#        alternative_data[row['application_id']]['unsuccesful_payments'] =  numeric_feats['unsuccesful_payments']
#
#        unsuccesful_amounts = list_feats['unsuccesful_amounts']
#        max_unsucc = None
#        min_unsucc = None
#        if unsuccesful_amounts:
#            if len(unsuccesful_amounts) > 0:
#                max_unsucc = max(unsuccesful_amounts)
#                min_unsucc = min(unsuccesful_amounts)
#            else:
#                max_unsucc = 0
#                min_unsucc = 0
#
#        alternative_data[row['application_id']]['max_unsuccesful_loan_payment'] =  max_unsucc
#        alternative_data[row['application_id']]['min_unsuccesful_loan_payment'] =  min_unsucc
#
#        # rejected loans
#        alternative_data[row['application_id']]['rejected_loans'] =  numeric_feats['rejected_loans']
#
#        # loan amounts detected
#        loan_amounts =  list_feats['loan_amounts']
#        max_loan_amount = None
#        if loan_amounts:
#            if len(loan_amounts) > 0:
#                max_loan_amount = max(loan_amounts)
#            else:
#                max_loan_amount = 0
#
#        alternative_data[row['application_id']]['max_loan_amount'] = max_loan_amount
#        alternative_data[row['application_id']]['loan_senders'] = list_feats['loan_senders']

        count += 1

        if count % 1000 == 0:
            print(count)


# convetr dict to pd df
additional_data = pd.DataFrame.from_dict(alternative_data, orient='index')
additional_data.index.names = ['application_id']
additional_data.reset_index(inplace=True, level= 0)

all_data = applications.merge(additional_data, on='application_id')
all_data.to_csv('applications_with_data_test.csv')

print("Done")

#!/usr/bin/python3.9
"""
This script downloads batch voucher files exported from FOLIO, generates CSV files from them\
to be loaded into Lawson for AP, and generates a report to be emailed to stakeholders.
"""

from ftplib import FTP
import paramiko
import csv
import re
import sys
import datetime
import json
import smtplib
import time
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

def main():
    """
    Calls functions to download the FOLIO batch voucher files and use them to produce an\
    invoice CSV and a distrib CSV for Lawson, as well as a human-readable report.\
    Then uploads the created files back to the FTP location and emails the report files.

    Returns
    -------
    None.
    """
    
    #Set appropriate load method here. Exactly one must be true, otherwise will return error.
    load_method = {}
    load_method['local'] = True
    load_method['ftp'] = False
    load_method['sftp'] = False
    
    try:
        folio_file_list = get_folio_files(load_method)
    except FileNotFoundError:
        print("No new voucher files found.")
        sys.exit()
    except NameError:
        print("Exactly one load method must be True.")
        sys.exit()
    invoice_files = create_invoice_csv(folio_file_list)
    distrib_files = create_distrib_csv(folio_file_list)
    report_files = create_email_report(folio_file_list)
    files_to_upload = invoice_files + distrib_files + report_files
    #upload_files(files_to_upload)
    email_recipients = ['tdannay@mtholyoke.edu'] #will need to add full list of recipients after testing
    #send_email(report_files, email_recipients)
    archive_used_json(folio_file_list) #add 'old' prefix to filename on local machine


def get_folio_files(load_method):
    """
    Searches for FOLIO batch voucher files based on a regex match of the filename.\
    If any exist, downloads them and then changes the name of the file at the FTP location so\
    it won't be captured the next time this program runs. If no matching file exists,\
    ends the program.
    
    Parameters
    ----------
    load_method : dict
        dictionary of options to configure for whether the data will be loaded from
        the local machine, an FTP location, or an SFTP location. Exactly one must be true.
        Keys must be 'local', 'ftp', and 'sftp'; values must be Boolean.
    
    Raises
    ------
    FileNotFoundError
        Raised when no file matching the FOLIO batch voucher filename convention is found.
        
    NameError
        Raised when the number of values set to True in load_method is not 1.

    Returns
    -------
    output : list
        List of filenames of FOLIO batch voucher files to be used by this program.
    """
    
    #Ensure exactly one load method was selected.
    true_count = 0
    for value in load_method.values():
        if value == True:
            true_count += 1
    if true_count != 1:
        raise NameError()
    
    #For use when taking the file(s) from the local machine. 
    if load_method['local'] == True:
        output = []
        local_files = os.listdir()
        local_files_list = match_filename(local_files, '^\d{4}-\d{2}-\d{2}T')
        for file in local_files_list:
            output.append(file)  
        if len(output) > 0:
            return output
        raise FileNotFoundError()
    
    #For use with the MHC SFTP server once FOLIO supports tranferring vouchers via SFTP instead of just FTP
    if load_method['sftp'] == True:   
        path = '' #SFTP location path                         
        output = []
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print('Connecting to MHC...')
        ssh_client.connect(hostname='', username='', password='') #SFTP account login info
        print("Connected.")
        sftp_client = ssh_client.open_sftp()  
        file_list = sftp_client.listdir(f'{path}/folio-json')
        filenames = match_filename(file_list, '^\d{4}-\d{2}-\d{2}T')
        for file in filenames:
            sftp_client.get(f'{path}/folio-json/{file}', file)
            print(f'Downloaded {file} from MHC')
            output.append(file)
            sftp_client.rename(f'{path}/folio-json/{file}', f'{path}/folio-json/old.{file}')
        sftp_client.close()
        ssh_client.close()
        if len(output) > 0:
            return output
        raise FileNotFoundError()
    
    #For use with the EBSCO FTP server which we had been using during testing period
    if load_method['ftp'] == True:
        ftp = FTP('') #FTP host name
        ftp.login(user='', passwd='') #FTP login credentials
        filename_list = ftp.nlst()
        filenames = match_filename(filename_list)
        output = []
        print('Downloading voucher files:')
        for file in filenames:
            ftp.retrbinary(f'RETR {file}', open(file, 'wb').write)
            print(f'Downloaded {file}')
            output.append(file)
            ftp.rename(file, f'old.{file}')  
        ftp.quit()
        if len(output) > 0:
            return output
        raise FileNotFoundError()


def create_invoice_csv(bv_files):
    """
    Takes FOLIO batch voucher files and produces a pandas dataframe of invoice data\
    for export to CSV in a format acceptable to Lawson.

    Parameters
    ----------
    bv_files : list
        List of names of FOLIO batch voucher files to be opened and converted to CSV

    Returns
    -------
    output_file_list : list
        List of filenames for CSV file(s) created.
    """

    current_date = datetime.date.today()
    output_file_list = []
    for index,file in enumerate(bv_files):
        df = pd.read_json(file)
        df = pd.concat([df.drop(['batchedVouchers'], axis=1),
                        df['batchedVouchers'].apply(pd.Series)], axis=1)
        df = df[df.status != 'Cancelled']
        if df.empty:
            print(f'No non-cancelled invoices in file {file}. Skipping creation of invoice csv.')
            continue
        invoice_count = df.shape[0]
        total_amount = '%.2f' % abs(df['amount'].sum())
        df = df[['accountingCode','amount','invoiceDate','vendorInvoiceNo']]
        df['invoiceDate'] = df['invoiceDate'].str.slice(start=0,stop=10).str.replace('-','')
        df['vendorInvoiceNo'] = df['vendorInvoiceNo'].apply(lambda x: x[2:] if x.startswith('MH') else x)
        df['VINandInvDate'] = df.apply(merge_vin_and_inv_date, field_length=22, axis=1)
        df['vendorInvoiceNo'] = df.apply(add_space_to_vin, field_length=15, axis=1)
        df['accountingCodeSuffix'] = df.apply(extract_ac_suffix, axis=1)
        df['accountingCode'] = (' ' + df['accountingCode'].str.split('_').str[0])
        df['creditdebit'] = ['C' if x < 0 else '' for x in df['amount']]
        #Add emtpy columns and reorder
        df = df.reindex(columns=['a','accountingCode','b','vendorInvoiceNo',
                        'c','d','e','f','g','h','creditdebit','i','j','invoiceDate',
                        'k','l','m','n','o','p','VINandInvDate','q','amount',
                        'r','s','t','u','v','w','x','y','z','aa','bb','accountingCodeSuffix'])
        #fill in constants
        df['a'] = '10'
        df['f'] = 'LBR'
        df['g'] = '10'
        #add quotes to columns as needed, since csv export parameters don't
        #allow sufficiently granular control over this
        cols_needing_quotes = ['a','accountingCode','vendorInvoiceNo','f','g',
                               'creditdebit','VINandInvDate','accountingCodeSuffix']
        add_quotes(df, cols_needing_quotes)
        df = df.fillna("")
        
        header = ['"$$$"','"LibraryFolio"',current_date.strftime("%Y%m%d"),
                  '"FOLIO UPLOAD FOR APCINVOICE"','"Y"','"AP"',
                  str(invoice_count).rjust(5,"0"),str(total_amount).rjust(10,"0"),'"SCOLGLAZ"']
        invoice_file = output_to_csv(df, header, index, invoice=True)
        output_file_list.append(invoice_file)
    return output_file_list


def create_distrib_csv(bv_files):
    """
    Takes FOLIO batch voucher files and produces a pandas dataframe of fund distribution data\
    for export to CSV in a format acceptable to Lawson.

    Parameters
    ----------
    bv_files : list
        List of names of FOLIO batch voucher files to be opened and converted to CSV

    Returns
    -------
    output_file_list : list
        List of filenames for CSV file(s) created.
    """

    current_date = datetime.date.today()
    output_file_list = []
    for index, file in enumerate(bv_files):
        df = pd.read_json(file)
        df = pd.concat([df.drop(['batchedVouchers'], axis=1),
                        df['batchedVouchers'].apply(pd.Series)], axis=1)
        df = df[df.status != 'Cancelled']
        if df.empty:
            print(f'No non-cancelled invoices in file {file}. Skipping creation of distrib csv.')
            continue
        droplist = ['amount','batchGroup','created','start','end','totalRecords',
                    'accountNo','vendorName','type','status','vendorAddress',
                    'adjustments','voucherNumber','id','voucherDate','folioInvoiceNo',
                    'enclosureNeeded','exchangeRate','invoiceCurrency','systemCurrency']
        df = df.drop(droplist, axis=1)
        df = df.explode('batchedVoucherLines', ignore_index=True)
        df = df.join(pd.json_normalize(df['batchedVoucherLines'])).drop(
             columns=['batchedVoucherLines'])
        df['invoiceDate'] = df['invoiceDate'].str.slice(start=0,stop=10).str.replace('-','')
        df['accountingCode'] = (' ' + df['accountingCode'].str.split('_').str[0])
        row_count = df.shape[0]
        total_amount = '%.2f' % abs(df['amount'].sum())
        df['VINIndex'] = df.groupby('vendorInvoiceNo').cumcount() + 1
        df = df['externalAccountNumber'].str.split('-', expand=True).fillna('').rename(
            columns={i:col for i,col in enumerate(['EAN1','EAN2','EAN3','EAN4','EAN5'])}).join(df)
        df['vendorInvoiceNo'] = df['vendorInvoiceNo'].apply(lambda x: x[2:] if x.startswith('MH') else x)
        df['VINandInvDate'] = df.apply(merge_vin_and_inv_date, field_length=23, axis=1)
        df['vendorInvoiceNo'] = df.apply(add_space_to_vin, field_length=14, axis=1)
        df['EAN4'] = df['EAN4'].str[-2:]
        #Add emtpy columns and reorder
        df = df.reindex(columns=['a','accountingCode','b','vendorInvoiceNo',
                        'c','VINIndex','amount','d','e','f','EAN2','EAN3','EAN4',
                        'h','i','j','VINandInvDate','k','EAN5'])
        #fill in constants
        df['a'] = '10'
        df['e'] = '10'
        df['EAN5'] = df['EAN5'].fillna('')
        df['EAN5'] = df['EAN5'].apply(lambda x: '      ' if x == '' else x)
        cols_needing_quotes = ['a','accountingCode','vendorInvoiceNo','e',
                               'EAN2','EAN3','EAN4','VINandInvDate','EAN5']
        add_quotes(df, cols_needing_quotes)
        header = ['"$$$"','"LibraryFolio"',current_date.strftime("%Y%m%d"),
                  '"FOLIO UPLOAD FOR APCDISTRIB"','"Y"','"AP"',
                  str(row_count).rjust(5,"0"),str(total_amount).rjust(10,"0"),'"SCOLGLAZ"']
        distrib_file = output_to_csv(df, header, index, distrib=True)
        output_file_list.append(distrib_file)
    return output_file_list


def create_email_report(bv_files):
    """
    Generates txt report to be emailed to stakeholders upon successful creation of Lawson\
    invoice/distrib CSV files.

    Parameters
    ----------
    bv_files : list
        List of names of FOLIO batch voucher files to be opened and converted to a report.

    Returns
    -------
    output_file_list : list
        List of filenames for txt files created.
    """

    current_date = datetime.date.today()
    output_file_list = []
    for index,file in enumerate(bv_files):
        report_list = []
        filename_suffix = time.strftime("%Y%m%d-%H%M%S")
        f = open(file)
        voucher_file = json.load(f)
        grand_total = 0
        invoice_count = 0
        batch = voucher_file.get('batchedVouchers')
        for voucher in batch:
            if voucher.get('status') == 'Cancelled':
                continue
            else:
                accounting_code = voucher.get('accountingCode')
                total_amount = '%.2f' % voucher.get('amount')
                grand_total += voucher.get('amount')
                if voucher.get('amount') >= 0:
                    credit_debit = 'D'
                else:
                    credit_debit = 'C'
                invoice_date = voucher.get('invoiceDate')[0:10]
                folio_inv_num = voucher.get('folioInvoiceNo')
                folio_voucher_num = voucher.get('voucherNumber')
                vendor_inv_number = voucher.get('vendorInvoiceNo')
                if vendor_inv_number.startswith('MH'):
                    vendor_inv_number = vendor_inv_number[2:]
                vendor_name = voucher.get('vendorName')
                lines = voucher.get('batchedVoucherLines')
                report_list.append('******** INVOICE REPORT TOTALED BY EXTERNAL FUND CODE ********\n')
                report_list.append(f'    Vendor Invoice Number:         {vendor_inv_number}\n')
                report_list.append(f'    Vendor:                        {vendor_name}\n')
                report_list.append(f'    Accounting Code:               {accounting_code}\n')
                report_list.append(f'    FOLIO Voucher Number:          {folio_voucher_num}\n')
                report_list.append(f'    FOLIO Invoice Number:          {folio_inv_num}\n')
                report_list.append(f'    Report Date:                   {current_date}\n')
                report_list.append(f'    Invoice Date:                  {invoice_date}\n')
                report_list.append(f'    Invoice Total:                 {total_amount}\n')
                report_list.append(f'    Credit/Debit:                  {credit_debit}\n')
                report_list.append('    External Fund                       Amount     \n')
                report_list.append('    ------------------------------      ------------\n')
                invoice_count += 1
                for line in lines:
                    amount = '%.2f' % line.get('amount')
                    external_account_num = line.get('externalAccountNumber')
                    report_list.append(f'    {external_account_num:<35} {amount}\n')
                report_list.append('\n\n')
            grand_total_str = '%.2f' % grand_total
            report_list.append(f'Total number of invoices: {invoice_count}\n')
            report_list.append(f'Grand total amount: {grand_total_str}')
            
            if report_list:
                filename = f'voucher_report_{filename_suffix}_{index}.txt'
                report = open(filename, 'w')
                report.writelines(report_list)
                report.close()
                output_file_list.append(filename)
        f.close()
    return output_file_list


def output_to_csv(df, header, index, invoice=False, distrib=False):
    """
    Creates CSV file formatted for Lawson from a given dataframe and header.

    Parameters
    ----------
    df : pandas dataframe
        The dataframe to be exported to CSV
    header : list
        The first line of the CSV - note that this header does not represent column headings as in\
        a conventional CSV structure; it is a separate set of values to which the actual CSV is\
        appended without column headers.
    index : int
        A number representing each file, starting with zero - for when multiple FOLIO files are\
        being processed simultaneously.
    invoice : bool, optional
        Set to True when outputting an invoice CSV. The default is False.
    distrib : bool, optional
        Set to True when outputting a distrib CSV. The default is False.

    Raises
    ------
    NameError
        Raised when the invoice and distrib parameters are both true or both false.\
        Excatly one must be true.

    Returns
    -------
    filename : str
        Name of exported file.
    """

    filename_suffix = time.strftime("%Y.%m.%d+%H.%M.%S")
    if invoice and not distrib:
        filename = f'ii-folioap-apcinvoice-{filename_suffix}_{index}.txt'
    elif distrib and not invoice:
        filename = f'ii-folioap-apcdistrib-{filename_suffix}_{index}.txt'
    else:
        raise NameError('Must specify exactly one of invoice or distrib as true')
    with open(filename, 'w', newline='') as output:
        writer = csv.writer(output, quoting=csv.QUOTE_NONE, escapechar="~", quotechar="")
        writer.writerow(header)
    df.to_csv(filename, mode='a', header=False, index=False, float_format='%.2f',
              quoting=csv.QUOTE_NONE, escapechar="")
    return filename


def match_filename(file_list, fn_pattern='^bv_.{12}_Mount Holyoke_\d{4}-\d{2}-\d{2}'):
    """Uses regex to identify files that match the filename pattern exported by FOLIO\
    and returns a list of matches."""

    matched_files = [filename for filename in file_list if re.search(fn_pattern, filename)]
    return matched_files


def add_quotes(df, needs_quotes_list):
    """Adds quotation marks to a given list of dataframe columns."""

    df[needs_quotes_list] = df[needs_quotes_list].astype(str)
    df[needs_quotes_list] = '"' + df[needs_quotes_list] + '"'
    return df


def merge_vin_and_inv_date(row, field_length):
    """Determines the number of spaces to add to the merged VIN/invoiceDate field\
    so that it is always 22 chars, and returns the field properly formatted."""

    space_count = field_length - (len(row['vendorInvoiceNo']) + len(row['invoiceDate']))
    return row['vendorInvoiceNo'] + (' ' * space_count) + row['invoiceDate']


def add_space_to_vin(row, field_length):
    """Determines the number of spaces to add to the VIN so that the field is always 15 chars."""
    space_count = field_length - len(row['vendorInvoiceNo'])
    return row['vendorInvoiceNo'] + (' ' * space_count)


def extract_ac_suffix(row):
    """Searches accountingCode for a suffix like '_##' and if found, returns the last digit."""

    if re.search('_\d{2}$', row['accountingCode']):
        return row['accountingCode'][-1]
    return '  '


def upload_files(file_list):
    """Uploads list of files to SFTP location"""

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print('Connecting to MHC...')
    ssh_client.connect(hostname='', username='', password='') #SFTP account login info
    print("Connected.")
    sftp_client = ssh_client.open_sftp()
    for file in file_list:
        sftp_client.put(file, f'') #insert SFTP location in f-string
        print(f'Uploaded {file}')
    sftp_client.close()
    ssh_client.close()


def send_email(files, recipients):
    """
    Sends an email with attached files to a list of recipients.
    
    Parameters
    ----------
    files : list (of str)
        List of names of files to be emailed
    recipients : list (of str)
        List of email addresses of email recipients

    Returns
    -------
    None.

    """

    email_sender = '' #what email address to send from?
    email_password = '' #password
    subject = 'MHC FOLIO Invoice Report'
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = email_sender
    message['To'] = ', '.join(recipients)
    for file in files:
        attachment = open(file, 'rb')
        obj = MIMEBase('application', 'octet-stream')
        obj.set_payload((attachment).read())
        encoders.encode_base64(obj)
        obj.add_header('Content-Disposition', 'attachment; filename = ' + file)
        message.attach(obj)
    message_to_send = message.as_string()
    email_session = smtplib.SMTP('smtp.gmail.com',587)
    email_session.starttls()
    email_session.login(email_sender, email_password)
    email_session.sendmail(email_sender, recipients, message_to_send)
    email_session.quit()
    print("Email sent")
 
    
def archive_used_json(bv_files):
    '''Adds a 'old.' prefix to the filename of the JSON file in the active local directory'''
    
    for file in bv_files:
        os.rename(file, f'old.{file}')


if __name__ == '__main__':
    main()

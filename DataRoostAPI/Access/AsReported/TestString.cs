using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using DataRoostAPI.Common.Models.AsReported;
using FactSet.Data.SqlClient;
using DataRoostAPI.Common.Models.TINT;
using System.Text;
using System.Text.RegularExpressions;
using FactSet.Fundamentals.Sourcelinks;
using Nest;
using System.Configuration;
using DataRoostAPI.Common.Models.SuperFast;
using System.Web.Mvc;
using System.Globalization;
using ExpressionStore.Components;

namespace CCS.Fundamentals.DataRoostAPI.Access.AsReported {

	public partial class JsonToSQL {
		public static string Json_UpdateTDPExample = @"
{
  'CompanyFinancialTerm': [
    {
      'action': 'delete',
      'obj': {
        'clone': {
          'ID': 483095,
          'DocumentSeries': {
            'ID': 3579,
            'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
            'Iconum': 10702,
            'Priority': '4',
            'Industry': 'COMMERCIAL',
            'CompanyName': 'BOVIE MEDICAL CORP',
            'FiscalYearEndMonth': 'December'
          },
          'TermStatusID': 1,
          'Description': 'Cash and cash equivalents',
          'NormalizedFlag': false,
          'EncoreTermFlag': 2,
          'IsDirty': true
        },
        'ID': 483095,
        'DocumentSeries': {
          'ID': 3579,
          'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
          'Iconum': 10702,
          'Priority': '4',
          'Industry': 'COMMERCIAL',
          'CompanyName': 'BOVIE MEDICAL CORP',
          'FiscalYearEndMonth': 'December'
        },
        'TermStatusID': 1,
        'Description': 'Cash and cash equivalents',
        'NormalizedFlag': false,
        'EncoreTermFlag': 2,
        'IsDirty': true
      }
    }
  ],
  'TableCell': [
    {
      'action': 'delete',
      'obj': {
        'clone': {
          'ID': 891610852,
          'Offset': 'o21509|l6|r10',
          'CellPeriodType': 'PIT',
          'PeriodTypeID': 'P',
          'CellPeriodCount': '0',
          'PeriodLength': 0,
          'CellDay': '31',
          'CellMonth': '12',
          'CellYear': '2016',
          'CellDate': '2016-12-31T00:00:00',
          'Value': '14,456',
          'CompanyFinancialTerm': {
            'clone': {
              'ID': 483095,
              'DocumentSeries': {
                'ID': 3579,
                'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
                'Iconum': 10702,
                'Priority': '4',
                'Industry': 'COMMERCIAL',
                'CompanyName': 'BOVIE MEDICAL CORP',
                'FiscalYearEndMonth': 'December'
              },
              'TermStatusID': 1,
              'Description': 'Cash and cash equivalents',
              'NormalizedFlag': false,
              'EncoreTermFlag': 2,
              'IsDirty': true
            },
            'ID': 483095,
            'DocumentSeries': {
              'ID': 3579,
              'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
              'Iconum': 10702,
              'Priority': '4',
              'Industry': 'COMMERCIAL',
              'CompanyName': 'BOVIE MEDICAL CORP',
              'FiscalYearEndMonth': 'December'
            },
            'TermStatusID': 1,
            'Description': 'Cash and cash equivalents',
            'NormalizedFlag': false,
            'EncoreTermFlag': 2,
            'IsDirty': false
          },
          'ValueNumeric': 14456.00000,
          'NormalizedNegativeIndicator': false,
          'ScalingFactorID': 'T',
          'AsReportedScalingFactor': 'T',
          'Currency': '$',
          'CurrencyCode': 'USD',
          'Cusip': null,
          'ARDErrorTypeId': 0,
          'MTMWErrorTypeId': 0,
          'IsIncomePositive': true,
          'XBRLTag': 'us-gaap_CashAndCashEquivalentsAtCarryingValue',
          'DocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
          'Label': '[Current assets:]Cash and cash equivalents',
          'IsDirty': true
        },
        'TableDimensions': [],
        'Row': null,
        'Column': null,
        'ID': 891610852,
        'Offset': 'o21509|l6|r10',
        'CellPeriodType': 'PIT',
        'PeriodTypeID': 'P',
        'CellPeriodCount': '0',
        'PeriodLength': 0,
        'CellDay': '31',
        'CellMonth': '12',
        'CellYear': '2016',
        'CellDate': '2016-12-31T00:00:00',
        'Value': '14,456',
       'CompanyFinancialTerm': {
          'clone': {
            'ID': 483095,
            'DocumentSeries': {
              'ID': 3579,
              'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
              'Iconum': 10702,
              'Priority': '4',
              'Industry': 'COMMERCIAL',
              'CompanyName': 'BOVIE MEDICAL CORP',
              'FiscalYearEndMonth': 'December'
            },
            'TermStatusID': 1,
            'Description': 'Cash and cash equivalents',
            'NormalizedFlag': false,
            'EncoreTermFlag': 2,
            'IsDirty': true
          },
          'ID': 483095,
          'DocumentSeries': {
            'ID': 3579,
            'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
            'Iconum': 10702,
            'Priority': '4',
            'Industry': 'COMMERCIAL',
            'CompanyName': 'BOVIE MEDICAL CORP',
            'FiscalYearEndMonth': 'December'
          },
          'TermStatusID': 1,
          'Description': 'Cash and cash equivalents',
          'NormalizedFlag': false,
          'EncoreTermFlag': 2,
          'IsDirty': false
        },
        'ValueNumeric': 14456.00000,
        'NormalizedNegativeIndicator': false,
        'ScalingFactorID': 'T',
        'AsReportedScalingFactor': 'T',
        'Currency': '$',
        'CurrencyCode': 'USD',
        'Cusip': null,
        'ARDErrorTypeId': 0,
        'MTMWErrorTypeId': 0,
        'IsIncomePositive': true,
        'XBRLTag': 'us-gaap_CashAndCashEquivalentsAtCarryingValue',
        'DocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
        'Label': '[Current assets:]Cash and cash equivalents',
        'IsDirty': true
      }
    },
    {
      'action': 'delete',
      'obj': {
        'clone': {
          'ID': 891610851,
          'Offset': 'o21313|l5|r10',
          'CellPeriodType': 'PIT',
          'PeriodTypeID': 'P',
          'CellPeriodCount': '0',
          'PeriodLength': 0,
          'CellDay': '31',
          'CellMonth': '12',
          'CellYear': '2017',
          'CellDate': '2017-12-31T00:00:00',
          'Value': '9,949',
          'CompanyFinancialTerm': {
            'clone': {
              'ID': 483095,
              'DocumentSeries': {
                'ID': 3579,
                'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
                'Iconum': 10702,
                'Priority': '4',
                'Industry': 'COMMERCIAL',
                'CompanyName': 'BOVIE MEDICAL CORP',
                'FiscalYearEndMonth': 'December'
              },
              'TermStatusID': 1,
              'Description': 'Cash and cash equivalents',
              'NormalizedFlag': false,
              'EncoreTermFlag': 2,
              'IsDirty': true
            },
            'ID': 483095,
            'DocumentSeries': {
              'ID': 3579,
              'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
              'Iconum': 10702,
              'Priority': '4',
              'Industry': 'COMMERCIAL',
              'CompanyName': 'BOVIE MEDICAL CORP',
              'FiscalYearEndMonth': 'December'
            },
            'TermStatusID': 1,
            'Description': 'Cash and cash equivalents',
            'NormalizedFlag': false,
            'EncoreTermFlag': 2,
            'IsDirty': false
          },
          'ValueNumeric': 9949.00000,
          'NormalizedNegativeIndicator': false,
          'ScalingFactorID': 'T',
          'AsReportedScalingFactor': 'T',
          'Currency': '$',
          'CurrencyCode': 'USD',
          'Cusip': null,
          'ARDErrorTypeId': 0,
          'MTMWErrorTypeId': 0,
          'IsIncomePositive': true,
          'XBRLTag': 'us-gaap_CashAndCashEquivalentsAtCarryingValue',
          'DocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
          'Label': '[Current assets:]Cash and cash equivalents',
          'IsDirty': true
        },
        'TableDimensions': [],
        'Row': null,
        'Column': null,
        'ID': 891610851,
        'Offset': 'o21313|l5|r10',
        'CellPeriodType': 'PIT',
        'PeriodTypeID': 'P',
        'CellPeriodCount': '0',
        'PeriodLength': 0,
        'CellDay': '31',
        'CellMonth': '12',
        'CellYear': '2017',
        'CellDate': '2017-12-31T00:00:00',
        'Value': '9,949',
        'CompanyFinancialTerm': {
          'clone': {
            'ID': 483095,
            'DocumentSeries': {
              'ID': 3579,
              'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
              'Iconum': 10702,
              'Priority': '4',
              'Industry': 'COMMERCIAL',
              'CompanyName': 'BOVIE MEDICAL CORP',
              'FiscalYearEndMonth': 'December'
           },
            'TermStatusID': 1,
            'Description': 'Cash and cash equivalents',
            'NormalizedFlag': false,
            'EncoreTermFlag': 2,
            'IsDirty': true
          },
          'ID': 483095,
          'DocumentSeries': {
            'ID': 3579,
            'PrimaryDocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
            'Iconum': 10702,
            'Priority': '4',
            'Industry': 'COMMERCIAL',
            'CompanyName': 'BOVIE MEDICAL CORP',
            'FiscalYearEndMonth': 'December'
          },
          'TermStatusID': 1,
          'Description': 'Cash and cash equivalents',
          'NormalizedFlag': false,
          'EncoreTermFlag': 2,
          'IsDirty': false
        },
        'ValueNumeric': 9949.00000,
        'NormalizedNegativeIndicator': false,
        'ScalingFactorID': 'T',
        'AsReportedScalingFactor': 'T',
        'Currency': '$',
        'CurrencyCode': 'USD',
        'Cusip': null,
        'ARDErrorTypeId': 0,
        'MTMWErrorTypeId': 0,
        'IsIncomePositive': true,
        'XBRLTag': 'us-gaap_CashAndCashEquivalentsAtCarryingValue',
        'DocumentId': '4397f929-f6e7-4277-a83f-f6d16ce387fb',
        'Label': '[Current assets:]Cash and cash equivalents',
        'IsDirty': true
      }
    }
  ],
  'TableDimension': [
    {
      'action': 'delete',
      'obj': {
        'clone': {
          'ID': 1893615047,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Cash and cash equivalents',
          'OrigLabel': '[Current assets:]Cash and cash equivalents',
          'Location': 0,
          'EndLocation': 0,
          'InsertedRow': false,
          'AdjustedOrder': 0,
          'IsDirty': true
        },
        'ID': 1893615047,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Cash and cash equivalents',
        'OrigLabel': '[Current assets:]Cash and cash equivalents',
        'Location': 0,
        'EndLocation': 0,
        'InsertedRow': false,
        'AdjustedOrder': 0,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615050,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Restricted cash',
          'OrigLabel': '[Current assets:]Restricted cash',
          'Location': 1,
          'EndLocation': 1,
          'InsertedRow': false,
          'AdjustedOrder': 1,
          'IsDirty': true
        },
        'ID': 1893615050,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Restricted cash',
        'OrigLabel': '[Current assets:]Restricted cash',
        'Location': 1,
        'EndLocation': 1,
        'InsertedRow': false,
        'AdjustedOrder': 0,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615051,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Trade accounts receivable, net of allowance of $204 and $118',
          'OrigLabel': '[Current assets:]Trade accounts receivable, net of allowance of $204 and $118',
          'Location': 2,
          'EndLocation': 2,
          'InsertedRow': false,
          'AdjustedOrder': 2,
          'IsDirty': true
        },
        'ID': 1893615051,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Trade accounts receivable, net of allowance of $204 and $118',
        'OrigLabel': '[Current assets:]Trade accounts receivable, net of allowance of $204 and $118',
        'Location': 2,
        'EndLocation': 2,
        'InsertedRow': false,
        'AdjustedOrder': 1,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615052,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Inventories, net',
          'OrigLabel': '[Current assets:]Inventories, net',
          'Location': 3,
          'EndLocation': 3,
          'InsertedRow': false,
          'AdjustedOrder': 3,
          'IsDirty': true
        },
        'ID': 1893615052,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Inventories, net',
        'OrigLabel': '[Current assets:]Inventories, net',
        'Location': 3,
        'EndLocation': 3,
        'InsThe thread 0x5298 has exited with code 0 (0x0).
ertedRow': false,
        'AdjustedOrder': 2,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615053,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Prepaid expenses and other current assets',
          'OrigLabel': '[Current assets:]Prepaid expenses and other current assets',
          'Location': 4,
          'EndLocation': 4,
          'InsertedRow': false,
          'AdjustedOrder': 4,
          'IsDirty': true
        },
        'ID': 1893615053,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Prepaid expenses and other current assets',
        'OrigLabel': '[Current assets:]Prepaid expenses and other current assets',
        'Location': 4,
        'EndLocation': 4,
        'InsertedRow': false,
        'AdjustedOrder': 3,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615054,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Total current assets',
          'OrigLabel': '[Current assets:]Total current assets',
          'Location': 5,
          'EndLocation': 5,
          'InsertedRow': false,
          'AdjustedOrder': 5,
          'IsDirty': true
        },
        'ID': 1893615054,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Total current assets',
        'OrigLabel': '[Current assets:]Total current assets',
        'Location': 5,
        'EndLocation': 5,
        'InsertedRow': false,
        'AdjustedOrder': 4,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
       'clone': {
          'ID': 1893615055,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Property and equipment, net',
          'OrigLabel': '[Current assets:]Property and equipment, net',
          'Location': 6,
          'EndLocation': 6,
          'InsertedRow': false,
          'AdjustedOrder': 6,
          'IsDirty': true
        },
        'ID': 1893615055,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Property and equipment, net',
        'OrigLabel': '[Current assets:]Property and equipment, net',
        'Location': 6,
        'EndLocation': 6,
        'InsertedRow': false,
        'AdjustedOrder': 5,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615056,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Brand name and trademark',
          'OrigLabel': '[Current assets:]Brand name and trademark',
          'Location': 7,
          'EndLocation': 7,
          'InsertedRow': false,
          'AdjustedOrder': 7,
          'IsDirty': true
        },
        'ID': 1893615056,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Brand name and trademark',
        'OrigLabel': '[Current assets:]Brand name and trademark',
        'Location': 7,
        'EndLocation': 7,
        'InsertedRow': false,
        'AdjustedOrder': 6,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615057,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Purchased technology and license rights, net',
          'OrigLabel': '[Current assets:]Purchased technology and license rights, net',
          'Location': 8,
          'EndLocation': 8,
          'InsertedRow': false,
          'AdjustedOrder': 8,
          'IsDirty': true
        },
        'ID': 1893615057,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Purchased technology and license rights, net',
        'OrigLabel': '[Current assets:]Purchased technology and license rights, net',
        'Location': 8,
        'EndLocation': 8,
        'InsertedRow': false,
        'AdjustedOrder': 7,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615058,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Goodwill',
          'OrigLabel': '[Current assets:]Goodwill',
          'Location': 9,
          'EndLocation': 9,
          'InsertedRow': false,
          'AdjustedOrder': 9,
          'IsDirty': true
        },
        'ID': 1893615058,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Goodwill',
        'OrigLabel': '[Current assets:]Goodwill',
        'Location': 9,
        'EndLocation': 9,
        'InsertedRow': false,
        'AdjustedOrder': 8,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615059,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Deposits',
          'OrigLabel': '[Current assets:]Deposits',
          'Location': 10,
          'EndLocation': 10,
          'InsertedRow': false,
          'AdjustedOrder': 10,
          'IsDirty': true
        },
        'ID': 1893615059,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Deposits',
        'OrigLabel': '[Current assets:]Deposits',
        'Location': 10,
        'EndLocation': 10,
        'InsertedRow': false,
        'AdjustedOrder': 9,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615060,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Other assets',
          'OrigLabel': '[Current assets:]Other assets',
          'Location': 11,
          'EndLocation': 11,
          'InsertedRow': false,
          'AdjustedOrder': 11,
          'IsDirty': true
        },
        'ID': 1893615060,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Other assets',
        'OrigLabel': '[Current assets:]Other assets',
        'Location': 11,
        'EndLocation': 11,
        'InsertedRow': false,
        'AdjustedOrder': 10,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615061,
          'DimensionTypeId': 1,
          'Label': '[Current assets:]Total assets',
          'OrigLabel': '[Current assets:]Total assets',
          'Location': 12,
          'EndLocation': 12,
          'InsertedRow': false,
          'AdjustedOrder': 12,
          'IsDirty': true
        },
        'ID': 1893615061,
        'DimensionTypeId': 1,
        'Label': '[Current assets:]Total assets',
        'OrigLabel': '[Current assets:]Total assets',
        'Location': 12,
        'EndLocation': 12,
        'InsertedRow': false,
        'AdjustedOrder': 11,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615062,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Accounts payable',
          'OrigLabel': '[Current liabilities:]Accounts payable',
          'Location': 13,
          'EndLocation': 13,
          'InsertedRow': false,
          'AdjustedOrder': 13,
          'IsDirty': true
        },
        'ID': 1893615062,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Accounts payable',
        'OrigLabel': '[Current liabilities:]Accounts payable',
        'Location': 13,
        'EndLocation': 13,
        'InsertedRow': false,
        'AdjustedOrder': 12,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615063,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Accrued severance and related',
          'OrigLabel': '[Current liabilities:]Accrued severance and related',
          'Location': 14,
          'EndLocation': 14,
          'InsertedRow': false,
          'AdjustedOrder': 14,
          'IsDirty': true
        },
        'ID': 1893615063,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Accrued severance and related',
        'OrigLabel': '[Current liabilities:]Accrued severance and related',
        'Location': 14,
        'EndLocation': 14,
        'InsertedRow': false,
        'AdjustedOrder': 13,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615064,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Accrued payroll',
          'OrigLabel': '[Current liabilities:]Accrued payroll',
          'Location': 15,
          'EndLocation': 15,
          'InsertedRow': false,
          'AdjustedOrder': 15,
          'IsDirty': true
        },
        'ID': 1893615064,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Accrued payroll',
        'OrigLabel': '[Current liabilities:]Accrued payroll',
        'Location': 15,
        'EndLocation': 15,
        'InsertedRow': false,
        'AdjustedOrder': 14,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615065,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Accrued vacation',
          'OrigLabel': '[Current liabilities:]Accrued vacation',
          'Location': 16,
          'EndLocation': 16,
          'InsertedRow': false,
          'AdjustedOrder': 16,
          'IsDirty': true
        },
        'ID': 1893615065,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Accrued vacation',
        'OrigLabel': '[Current liabilities:]Accrued vacation',
        'Location': 16,
        'EndLocation': 16,
        'InsertedRow': false,
        'AdjustedOrder': 15,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615066,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Current portion of mortgage note payable',
          'OrigLabel': '[Current liabilities:]Current portion of mortgage note payable',
          'Location': 17,
          'EndLocation': 17,
          'InsertedRow': false,
          'AdjustedOrder': 17,
          'IsDirty': true
        },
        'ID': 1893615066,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Current portion of mortgage note payable',
        'OrigLabel': '[Current liabilities:]Current portion of mortgage note payable',
        'Location': 17,
        'EndLocation': 17,
        'InsertedRow': false,
        'AdjustedOrder': 16,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615067,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Accrued and other liabilities',
          'OrigLabel': '[Current liabilities:]Accrued and other liabilities',
          'Location': 18,
          'EndLocation': 18,
          'InsertedRow': false,
          'AdjustedOrder': 18,
          'IsDirty': true
        },
        'ID': 1893615067,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Accrued and other liabilities',
        'OrigLabel': '[Current liabilities:]Accrued and other liabilities',
        'Location': 18,
        'EndLocation': 18,
        'InsertedRow': false,
        'AdjustedOrder': 17,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615068,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Total current liabilities',
          'OrigLabel': '[Current liabilities:]Total current liabilities',
          'Location': 19,
          'EndLocation': 19,
          'InsertedRow': false,
          'AdjustedOrder': 19,
          'IsDirty': true
        },
        'ID': 1893615068,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Total current liabilities',
        'OrigLabel': '[Current liabilities:]Total current liabilities',
        'Location': 19,
        'EndLocation': 19,
        'InsertedRow': false,
        'AdjustedOrder': 18,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615069,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Mortgage note payable, net of current portion',
          'OrigLabel': '[Current liabilities:]Mortgage note payable, net of current portion',
          'Location': 20,
          'EndLocation': 20,
          'InsertedRow': false,
          'AdjustedOrder': 20,
          'IsDirty': true
        },
        'ID': 1893615069,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Mortgage note payable, net of current portion',
        'OrigLabel': '[Current liabilities:]Mortgage note payable, net of current portion',
        'Location': 20,
        'EndLocation': 20,
        'InsertedRow': false,
        'AdjustedOrder': 19,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615070,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Note payable',
          'OrigLabel': '[Current liabilities:]Note payable',
          'Location': 21,
          'EndLocation': 21,
          'InsertedRow': false,
          'AdjustedOrder': 21,
          'IsDirty': true
        },
        'ID': 1893615070,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Note payable',
        'OrigLabel': '[Current liabilities:]Note payable',
        'Location': 21,
        'EndLocation': 21,
        'InsertedRow': false,
        'AdjustedOrder': 20,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615071,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Deferred rents',
          'OrigLabel': '[Current liabilities:]Deferred rents',
          'Location': 22,
          'EndLocation': 22,
          'InsertedRow': false,
          'AdjustedOrder': 22,
          'IsDirty': true
        },
        'ID': 1893615071,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Deferred rents',
        'OrigLabel': '[Current liabilities:]Deferred rents',
        'Location': 22,
        'EndLocation': 22,
        'InsertedRow': false,
        'AdjustedOrder': 21,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615072,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Deferred tax liability',
          'OrigLabel': '[Current liabilities:]Deferred tax liability',
          'Location': 23,
          'EndLocation': 23,
          'InsertedRow': false,
          'AdjustedOrder': 23,
          'IsDirty': true
        },
        'ID': 1893615072,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Deferred tax liability',
        'OrigLabel': '[Current liabilities:]Deferred tax liability',
        'Location': 23,
        'EndLocation': 23,
        'InsertedRow': false,
        'AdjustedOrder': 22,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615073,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Derivative liabilities',
          'OrigLabel': '[Current liabilities:]Derivative liabilities',
          'Location': 24,
          'EndLocation': 24,
          'InsertedRow': false,
          'AdjustedOrder': 24,
          'IsDirty': true
        },
        'ID': 1893615073,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Derivative liabilities',
        'OrigLabel': '[Current liabilities:]Derivative liabilities',
        'Location': 24,
        'EndLocation': 24,
        'InsertedRow': false,
        'AdjustedOrder': 23,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615074,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Total liabilities',
          'OrigLabel': '[Current liabilities:]Total liabilities',
          'Location': 25,
          'EndLocation': 25,
          'InsertedRow': false,
          'AdjustedOrder': 25,
          'IsDirty': true
        },
        'ID': 1893615074,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Total liabilities',
        'OrigLabel': '[Current liabilities:]Total liabilities',
        'Location': 25,
        'EndLocation': 25,
        'InsertedRow': false,
        'AdjustedOrder': 24,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615075,
          'DimensionTypeId': 1,
          'Label': '[Current liabilities:]Commitments and Contingencies (see Notes 9 and 11)',
          'OrigLabel': '[Current liabilities:]Commitments and Contingencies (see Notes 9 and 11)',
          'Location': 26,
          'EndLocation': 26,
          'InsertedRow': false,
          'AdjustedOrder': 26,
          'IsDirty': true
        },
        'ID': 1893615075,
        'DimensionTypeId': 1,
        'Label': '[Current liabilities:]Commitments and Contingencies (see Notes 9 and 11)',
        'OrigLabel': '[Current liabilities:]Commitments and Contingencies (see Notes 9 and 11)',
        'Location': 26,
        'EndLocation': 26,
        'InsertedRow': false,
        'AdjustedOrder': 25,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615076,
          'DimensionTypeId': 1,
          'Label': '[STOCKHOLDERS\' EQUITY]Series B convertible preferred stock, $0.001 par value; 3,588,139 authorized and zero issued and outstanding as of December 31, 2017 and 3,588,139 authorized and 975,639 issued and outstanding as of December 31, 2016, respectively',
          'OrigLabel': '[STOCKHOLDERS\' EQUITY]Series B convertible preferred stock, $0.001 par value; 3,588,139 authorized and zero issued and outstanding as of December 31, 2017 and 3,588,139 authorized and 975,639 issued and outstanding as of December 31, 2016, respectively',
          'Location': 27,
          'EndLocation': 27,
          'InsertedRow': false,
          'AdjustedOrder': 27,
          'IsDirty': true
        },
        'ID': 1893615076,
        'DimensionTypeId': 1,
        'Label': '[STOCKHOLDERS\' EQUITY]Series B convertible preferred stock, $0.001 par value; 3,588,139 authorized and zero issued and outstanding as of December 31, 2017 and 3,588,139 authorized and 975,639 issued and outstanding as of December 31, 2016, respectively',
        'OrigLabel': '[STOCKHOLDERS\' EQUITY]Series B convertible preferred stock, $0.001 par value; 3,588,139 authorized and zero issued and outstanding as of December 31, 2017 and 3,588,139 authorized and 975,639 issued and outstanding as of December 31, 2016, respectively',
        'Location': 27,
        'EndLocation': 27,
        'InsertedRow': false,
        'AdjustedOrder': 26,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615077,
          'DimensionTypeId': 1,
          'Label': '[STOCKHOLDERS\' EQUITY]Common stock, $0.001 par value; 75,000,000 shares authorized; 33,021,170 issued and 32,878,091 outstanding as of December 31, 2017 and 40,000,000 shares authorized; 31,002,832 issued and 30,859,753 outstanding as of December 31, 2016, respectively',
          'OrigLabel': '[STOCKHOLDERS\' EQUITY]Common stock, $0.001 par value; 75,000,000 shares authorized; 33,021,170 issued and 32,878,091 outstanding as of December 31, 2017 and 40,000,000 shares authorized; 31,002,832 issued and 30,859,753 outstanding as of December 31, 2016, respectively',
          'Location': 28,
          'EndLocation': 28,
          'InsertedRow': false,
          'AdjustedOrder': 28,
          'IsDirty': true
        },
        'ID': 1893615077,
        'DimensionTypeId': 1,
        'Label': '[STOCKHOLDERS\' EQUITY]Common stock, $0.001 par value; 75,000,000 shares authorized; 33,021,170 issued and 32,878,091 outstanding as of December 31, 2017 and 40,000,000 shares authorized; 31,002,832 issued and 30,859,753 outstanding as of December 31, 2016, respectively',
        'OrigLabel': '[STOCKHOLDERS\' EQUITY]Common stock, $0.001 par value; 75,000,000 shares authorized; 33,021,170 issued and 32,878,091 outstanding as of December 31, 2017 and 40,000,000 shares authorized; 31,002,832 issued and 30,859,753 outstanding as of December 31, 2016, respectively',
        'Location': 28,
        'EndLocation': 28,
        'InsertedRow': false,
        'AdjustedOrder': 27,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615078,
          'DimensionTypeId': 1,
          'Label': '[STOCKHOLDERS\' EQUITY]Additional paid-in capital',
          'OrigLabel': '[STOCKHOLDERS\' EQUITY]Additional paid-in capital',
          'Location': 29,
          'EndLocation': 29,
          'InsertedRow': false,
          'AdjustedOrder': 29,
          'IsDirty': true
        },
        'ID': 1893615078,
        'DimensionTypeId': 1,
        'Label': '[STOCKHOLDERS\' EQUITY]Additional paid-in capital',
        'OrigLabel': '[STOCKHOLDERS\' EQUITY]Additional paid-in capital',
        'Location': 29,
        'EndLocation': 29,
        'InsertedRow': false,
        'AdjustedOrder': 28,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615079,
          'DimensionTypeId': 1,
          'Label': '[STOCKHOLDERS\' EQUITY]Accumulated deficit',
          'OrigLabel': '[STOCKHOLDERS\' EQUITY]Accumulated deficit',
          'Location': 30,
          'EndLocation': 30,
          'InsertedRow': false,
          'AdjustedOrder': 30,
          'IsDirty': true
        },
        'ID': 1893615079,
        'DimensionTypeId': 1,
        'Label': '[STOCKHOLDERS\' EQUITY]Accumulated deficit',
        'OrigLabel': '[STOCKHOLDERS\' EQUITY]Accumulated deficit',
        'Location': 30,
        'EndLocation': 30,
        'InsertedRow': false,
        'AdjustedOrder': 29,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615080,
          'DimensionTypeId': 1,
          'Label': '[STOCKHOLDERS\' EQUITY]Total stockholders\' equity',
          'OrigLabel': '[STOCKHOLDERS\' EQUITY]Total stockholders\' equity',
          'Location': 31,
          'EndLocation': 31,
          'InsertedRow': false,
          'AdjustedOrder': 31,
          'IsDirty': true
        },
        'ID': 1893615080,
        'DimensionTypeId': 1,
        'Label': '[STOCKHOLDERS\' EQUITY]Total stockholders\' equity',
        'OrigLabel': '[STOCKHOLDERS\' EQUITY]Total stockholders\' equity',
        'Location': 31,
        'EndLocation': 31,
        'InsertedRow': false,
        'AdjustedOrder': 30,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615081,
          'DimensionTypeId': 1,
          'Label': '[STOCKHOLDERS\' EQUITY]Total liabilities and stockholders\' equity',
          'OrigLabel': '[STOCKHOLDERS\' EQUITY]Total liabilities and stockholders\' equity',
          'Location': 32,
          'EndLocation': 32,
          'InsertedRow': false,
          'AdjustedOrder': 32,
          'IsDirty': true
        },
        'ID': 1893615081,
        'DimensionTypeId': 1,
        'Label': '[STOCKHOLDERS\' EQUITY]Total liabilities and stockholders\' equity',
        'OrigLabel': '[STOCKHOLDERS\' EQUITY]Total liabilities and stockholders\' equity',
        'Location': 32,
        'EndLocation': 32,
        'InsertedRow': false,
        'AdjustedOrder': 31,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
     'obj': {
        'clone': {
          'ID': 1893615082,
          'DimensionTypeId': 1,
          'Label': '[Stockholders\' equity:]Preferred stock, shares authorized',
          'OrigLabel': '[Stockholders\' equity:]Preferred stock, shares authorized',
          'Location': 33,
          'EndLocation': 33,
          'InsertedRow': false,
          'AdjustedOrder': 33,
          'IsDirty': true
        },
        'ID': 1893615082,
        'DimensionTypeId': 1,
        'Label': '[Stockholders\' equity:]Preferred stock, shares authorized',
        'OrigLabel': '[Stockholders\' equity:]Preferred stock, shares authorized',
        'Location': 33,
        'EndLocation': 33,
        'InsertedRow': false,
        'AdjustedOrder': 32,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615083,
          'DimensionTypeId': 1,
          'Label': '[Stockholders\' equity:]Common stock, par value (in dollars per share)',
          'OrigLabel': '[Stockholders\' equity:]Common stock, par value (in dollars per share)',
          'Location': 34,
          'EndLocation': 34,
          'InsertedRow': false,
          'AdjustedOrder': 34,
          'IsDirty': true
       },
        'ID': 1893615083,
        'DimensionTypeId': 1,
        'Label': '[Stockholders\' equity:]Common stock, par value (in dollars per share)',
        'OrigLabel': '[Stockholders\' equity:]Common stock, par value (in dollars per share)',
        'Location': 34,
        'EndLocation': 34,
        'InsertedRow': false,
        'AdjustedOrder': 33,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615084,
          'DimensionTypeId': 1,
          'Label': '[Stockholders\' equity:]Common stock, shares authorized',
          'OrigLabel': '[Stockholders\' equity:]Common stock, shares authorized',
          'Location': 35,
          'EndLocation': 35,
          'InsertedRow': false,
          'AdjustedOrder': 35,
          'IsDirty': true
        },
        'ID': 1893615084,
        'DimensionTypeId': 1,
        'Label': '[Stockholders\' equity:]Common stock, shares authorized',
        'OrigLabel': '[Stockholders\' equity:]Common stock, shares authorized',
        'Location': 35,
        'EndLocation': 35,
        'InsertedRow': false,
        'AdjustedOrder': 34,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615085,
          'DimensionTypeId': 1,
          'Label': '[Stockholders\' equity:]Common stock, shares issued',
          'OrigLabel': '[Stockholders\' equity:]Common stock, shares issued',
          'Location': 36,
          'EndLocation': 36,
          'InsertedRow': false,
          'AdjustedOrder': 36,
          'IsDirty': true
        },
        'ID': 1893615085,
        'DimensionTypeId': 1,
        'Label': '[Stockholders\' equity:]Common stock, shares issued',
        'OrigLabel': '[Stockholders\' equity:]Common stock, shares issued',
        'Location': 36,
        'EndLocation': 36,
        'InsertedRow': false,
        'AdjustedOrder': 35,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615086,
          'DimensionTypeId': 1,
          'Label': '[Stockholders\' equity:]Common stock, shares outstanding',
          'OrigLabel': '[Stockholders\' equity:]Common stock, shares outstanding',
          'Location': 37,
          'EndLocation': 37,
          'InsertedRow': false,
          'AdjustedOrder': 37,
          'IsDirty': true
        },
        'ID': 1893615086,
        'DimensionTypeId': 1,
        'Label': '[Stockholders\' equity:]Common stock, shares outstanding',
        'OrigLabel': '[Stockholders\' equity:]Common stock, shares outstanding',
        'Location': 37,
        'EndLocation': 37,
        'InsertedRow': false,
        'AdjustedOrder': 36,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615087,
          'DimensionTypeId': 1,
          'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, par value (in dollars per share)',
          'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, par value (in dollars per share)',
          'Location': 38,
          'EndLocation': 38,
          'InsertedRow': false,
          'AdjustedOrder': 38,
          'IsDirty': true
        },
        'ID': 1893615087,
        'DimensionTypeId': 1,
        'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, par value (in dollars per share)',
        'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, par value (in dollars per share)',
        'Location': 38,
        'EndLocThe thread 0x4c40 has exited with code 0 (0x0).
ation': 38,
        'InsertedRow': false,
        'AdjustedOrder': 37,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615088,
          'DimensionTypeId': 1,
          'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares authorized',
          'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares authorized',
          'Location': 39,
          'EndLocation': 39,
          'InsertedRow': false,
          'AdjustedOrder': 39,
          'IsDirty': true
        },
        'ID': 1893615088,
        'DimensionTypeId': 1,
        'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares authorized',
        'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares authorized',
        'Location': 39,
        'EndLocation': 39,
        'InsertedRow': false,
        'AdjustedOrder': 38,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615089,
          'DimensionTypeId': 1,
          'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares issued',
          'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares issued',
          'Location': 40,
          'EndLocation': 40,
          'InsertedRow': false,
          'AdjustedOrder': 40,
          'IsDirty': true
        },
        'ID': 1893615089,
        'DimensionTypeId': 1,
        'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares issued',
        'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares issued',
        'Location': 40,
        'EndLocation': 40,
       'InsertedRow': false,
        'AdjustedOrder': 39,
        'IsDirty': true
      }
    },
    {
      'action': 'update',
      'obj': {
        'clone': {
          'ID': 1893615090,
          'DimensionTypeId': 1,
          'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares outstanding',
          'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares outstanding',
          'Location': 41,
          'EndLocation': 41,
          'InsertedRow': false,
          'AdjustedOrder': 41,
          'IsDirty': true
        },
        'ID': 1893615090,
        'DimensionTypeId': 1,
        'Label': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares outstanding',
        'OrigLabel': '[Series B Preferred Stock][Stockholders\' equity:]Preferred stock, shares outstanding',
        'Location': 41,
        'EndLocation': 41,
        'InsertedRow': false,
        'AdjustedOrder': 40,
        'IsDirty': true
      }
    }
  ],
  'DobumenTable': []
}
";
	}
}
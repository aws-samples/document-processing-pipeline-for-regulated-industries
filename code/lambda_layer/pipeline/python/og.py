import json
from helper import FileHelper, S3Helper
from trp import Document
import boto3

class OutputGenerator:
    
    def __init__(self, response, forms, tables, **kwargs):
        self.response = response
        self.forms = forms
        self.tables = tables
        self.documentId = kwargs.get("documentId", None)
        self.bucketName = kwargs.get("bucketName", None)
        self.objectName = kwargs.get("objectName", None)
        self.outputPath = "{}/ocr-analysis".format(self.objectName)
        self.document = Document(self.response)

    def _outputText(self, page, p, no_write=False):
        text = page.text
        textInReadingOrder = page.getTextInReadingOrder()
        
        if no_write:
            return (text, textInReadingOrder)
        else:
            opath = "{}/page-{}/text.txt".format(self.outputPath, p)
            opath = "{}/page-{}/text-inreadingorder.txt".format(self.outputPath, p)
            S3Helper.writeToS3(textInReadingOrder, self.bucketName, opath)
            S3Helper.writeToS3(text, self.bucketName, opath)

    def _outputForm(self, page, p, no_write=False):
        csvData = []
        for field in page.form.fields:
            csvItem  = []
            if(field.key):
                csvItem.append(field.key.text)
            else:
                csvItem.append("")
            if(field.value):
                csvItem.append(field.value.text)
            else:
                csvItem.append("")
            csvData.append(csvItem)
        if no_write:
            return csvData
        else:
            csvFieldNames = ['Key', 'Value']
            opath = "{}/page-{}/forms.csv".format(self.outputPath, p)
            S3Helper.writeCSV(csvFieldNames, csvData, self.bucketName, opath)

    def _outputTable(self, page, p, no_write=False):
        csvData = []
        for table in page.tables:
            csvRow = []
            csvRow.append("Table")
            csvData.append(csvRow)
            for row in table.rows:
                csvRow  = []
                for cell in row.cells:
                    csvRow.append(cell.text)
                csvData.append(csvRow)
            csvData.append([])
            csvData.append([])
        if no_write:
            return csvData
        else:
            opath = "{}/page-{}/tables.csv".format(self.outputPath, p)
            S3Helper.writeCSVRaw(csvData, self.bucketName, opath)

    def structurePageForm(self, page):
        return self._outputForm(page, 0, no_write=True)
            
    def structurePageTable(self, page):
        return self._outputTable(page, 0, no_write=True)
    
    def structurePageText(self, page):
        text, structuredText = self._outputText(page, 0, no_write=True)
        return structuredText

    def writeTextractOutputs(self, taggingStr=None):
        if not self.document.pages:
            return
        docText = ""
        p = 1
        for page in self.document.pages:
            opath = "{}/page-{}/response.json".format(self.outputPath, p)
            S3Helper.writeToS3(json.dumps(page.blocks), self.bucketName, opath, taggingStr)
            self._outputText(page, p)
            docText = docText + page.text + "\n"
            if(self.forms):
                self._outputForm(page, p)
            if(self.tables):
                self._outputTable(page, p)
            p = p + 1
        # Write the whole output for it to then be used for comprehend
        opath = "{}/fullresponse.json".format(self.outputPath)
        print("Total Pages in Document: {}".format(len(self.document.pages)))
        S3Helper.writeToS3(json.dumps(self.response), self.bucketName, opath, taggingStr)
import os
import re
import time
import pdfbox

#
# import winerror
# from win32com.client import Dispatch
# from win32com.client.dynamic import ERRORS_BAD_CONTEXT

# # try importing scandir and if found, use it as it's a few magnitudes of an order faster than stock os.walk
# try:
#     from scandir import walk
# except ImportError:
#     from os import walk


# def acrobat_extract_text(f_input, f_output):
#     avDoc = Dispatch("AcroExch.AVDoc")
#     ret = avDoc.Open(f_input, f_input)
#     assert ret
#     pdDoc = avDoc.GetPDDoc()
#     jsObject = pdDoc.GetJSObject()
#     try:
#         jsObject.SaveAs(f_output, "com.adobe.acrobat.plain-text")
#     except Exception :
#         print(Exception.with_traceback())
#         pass
#     pdDoc.Close()
#     del pdDoc
#     avDoc.Close(True)


if __name__ == "__main__":
    file = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
    state = True
    failed = []
    file_num = 0
    global ERRORS_BAD_CONTEXT
    # ERRORS_BAD_CONTEXT.append(winerror.E_NOTIMPL)
    p = pdfbox.PDFBox()
    while state:
        text = file.readline()
        if text == '':
            state = False
        text = text.strip()
        filesource = 'E:/Ninggar/Mgstr/penelitian/Data/files/files/' + (
            re.sub('_pdf', '.pdf', re.sub('(%20)|([\\._]+)', '_',
                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
        filetarget = 'E:/Ninggar/Mgstr/penelitian/Data/files/new_1_text_files/' + (
            re.sub('_pdf', '.txt', re.sub('(%20)|([\\._]+)', '_',
                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
        dirName = re.sub('/[^/]+$', '', filetarget)
        try:
            # Create target Directory
            os.makedirs(dirName)
            print("Directory ", dirName, " Created ")
        except Exception:
            None
        # print(filesource)
        # print(file_num)
        # filetarget='E:/test.txt'
        if not re.search('lamp(|iran)|pjl', filesource):
            try:
                f = open(filesource)
                f.close()
                p.extract_text(filesource, filetarget)
                # acrobat_extract_text(filesource, filetarget)
            except IOError:
                print("File not accessible : {}".format(filesource))
                continue
            except Exception:
                print('failed_extract', filesource)
                # print(Exception.with_traceback())
        file_num += 1
        print(file_num)
        # time.sleep(3)
        # break
    file.close()

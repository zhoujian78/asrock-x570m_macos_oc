# -*- coding: utf-8 -*-
import os
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo
import json
import threading
import wx
import sys
import oss2_client_fun as oss_func
from pubsub import pub


class up_file_Thread(threading.Thread):
    def __init__(self,key,filename,bucket,updict={}):
        self.breakflag = False
        self.bucket = bucket
        self.key = key
        self.filename = filename
        #self.parts2 = []
        #self.up_files = {}
        self.up_dict = updict
        threading.Thread.__init__(self)
        if sys.platform.find('win') == 0:
            self.json_file = os.getcwd()+ "\\upload\\" + oss_func.data_md5(self.key).replace("/", "_") + ".json"
        else:
            self.json_file=os.getcwd()+ "/upload/"+oss_func.data_md5(self.key).replace("/","_")+".json"
        self.start()

    def stop(self):
        self.breakflag = True


    def run(self):
        self.upfile()
        if self.key in self.up_dict:
            del self.up_dict[self.key]

    def Complete_upload(self,key,json_file,crc64,upload_id,up_files_dict):
        # 完成分片上传。
        # 如需在完成分片上传时设置文件访问权限ACL，请在complete_multipart_upload函数中设置相关headers，参考如下。
        # headers = dict()
        # headers["x-oss-object-acl"] = oss2.OBJECT_ACL_PRIVATE
        # bucket.complete_multipart_upload(key, upload_id, parts, headers=headers)
        parts2=[]
        for partinfo in up_files_dict["parts"]:
            partinfo =eval(partinfo)
            parts2.append(PartInfo(partinfo['part_number'], partinfo['etag']))
        result=self.bucket.complete_multipart_upload(key, upload_id, parts2)

        if not result.crc == crc64:
            dlg1 = wx.MessageDialog(
                None, u"上传文件校验失败！", u"上传文件校验失败", wx.OK | wx.ICON_QUESTION)
            if dlg1.ShowModal() == wx.ID_OK:
                dlg1.Destroy()
            msg = '%s\t(failed！) ' % (key)
            wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
            os.remove(json_file)
            return
        msg = '%s\t(finished)100 ' % (key)
        wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
        up_files_dict["finish"]=True
        up_files_dict["key"]=key
        up_json = open(json_file, "w", encoding='utf-8')
        json.dump(up_files_dict, up_json, indent=2, sort_keys=True, ensure_ascii=False)
        up_json.close()
        os.remove(json_file)

    def Resume_Upfile_Parts(self,json_file,crc64):
        with open(json_file, "r", encoding='utf-8') as f:
            data = json.load(f)

        f.close()
        if data["offset"]==0:
            
            self.Upfile_Parts(json_file,crc64,self.key,self.filename)
            return
        if not data["finish"]:
            key = data["key"]
            filename = data["file"]
            total_size = data["total_size"]
            part_num = data["part_num"]
            part_size = data["part_size"]
            upload_id = data["upload_id"]
            parts = data["parts"]
            encode_md5 = data["MD5"]
            md5 = oss_func.filemd5(filename)
            if not md5 == encode_md5:
                dlg1 = wx.MessageDialog(
                    None, u"源文件已改动\n上传文件校验失败！", u"源文件已改动", wx.OK | wx.ICON_QUESTION)
                if dlg1.ShowModal() == wx.ID_OK:
                    dlg1.Destroy()
                msg = '%s\t(failed！) ' % (key)
                wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
                os.remove(json_file)
                return
            with  open(filename, 'rb') as fileobj:
                part_number = 1
                offset = 0
                up_files_dict = data
                part_info = {}
                while offset < total_size:
                    num_to_upload = min(part_size, total_size - offset)
                    # 调用SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                    rate = int(100 * float(offset) / float(total_size))
                    msg = '%s\t(uploading)%i' % (key, rate)

                    wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
                    if part_number <= part_num:
                        part_number += 1
                        offset += num_to_upload
                        continue
                    fileobj.seek(offset)
                    result = self.bucket.upload_part(key, upload_id, part_number,
                                                     SizedFileAdapter(fileobj, num_to_upload))
                    part_info['part_number'] = part_number
                    part_info['etag'] = result.etag
                    parts.append(str(part_info))
                    offset += num_to_upload
                    up_files_dict["offset"] = offset
                    up_files_dict["part_num"] = part_number
                    rate = int(100 * float(offset) / float(total_size))
                    msg = '%s\t(uploading)%i' % (key, rate)
                    wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
                    part_number = part_number + 1
                    up_files_dict["key"] = key
                    up_files_dict["parts"] = parts
                    up_files_dict["finish"] = False
                    up_json = open(json_file, "w+", encoding='utf-8')
                    json.dump(up_files_dict, up_json, indent=2, sort_keys=True, ensure_ascii=False)
                    up_json.close()
                    if self.breakflag:
                        # self.main_win.cancel = False
                        msg = '%s\t(stoped)%i ' % (key, rate)

                        wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
                        return
                self.Complete_upload(key,json_file,crc64, upload_id, up_files_dict)
                return
        else:
            return

    def Upfile_Parts(self,json_file,crc64,key,filename):
        up_files_dict = {}
        encode_md5=oss_func.filemd5(filename)
        total_size = os.path.getsize(filename)
        if total_size==0:
            self.bucket.put_object_from_file (key,filename )
            msg = '%s\t(finished)100 ' % (key)
            wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
            os.remove(json_file)
            return
        # determine_part_size方法用于确定分片大小。
        part_size = determine_part_size(total_size, preferred_size=100 * 1024)
        upload_id = self.bucket.init_multipart_upload(key).upload_id
        up_files_dict["MD5"]=encode_md5
        up_files_dict["file"]=filename
        up_files_dict["upload_id"]=upload_id
        up_files_dict["total_size"]=total_size
        up_files_dict["part_size"]=part_size

        # 逐个上传分片。
        with open(filename, 'rb') as fileobj:
            part_number = 1
            offset = 0
            part_info={}
            parts=[]
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)

                # 调用SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                result = self.bucket.upload_part(key, upload_id, part_number,
                                            SizedFileAdapter(fileobj, num_to_upload))
                part_info['part_number']=part_number
                part_info['etag']=result.etag
                parts.append(str(part_info))
                offset += num_to_upload
                rate = int(100 * float(offset) / float(total_size))
                msg = '%s\t(uploading)%i' % (key, rate)
                wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
                up_files_dict["offset"]=offset
                up_files_dict["part_num"]=part_number
                part_number = part_number + 1
                up_files_dict["parts"]=parts
                up_files_dict["key"] = key
                up_files_dict["finish"]= False
                up_json = open(json_file, "w+", encoding='utf-8')
                json.dump(up_files_dict, up_json, indent=2, sort_keys=True, ensure_ascii=False)
                up_json.close()
                if self.breakflag :
                    #self.main_win.cancel=False
                    msg = '%s\t(stoped)%i ' % (key, rate)
                    wx.CallAfter(pub.sendMessage, "pgMsg", msg=msg)
                    return
        self.Complete_upload(key,json_file,crc64, upload_id, up_files_dict)



    def upfile(self):

        crc64 = oss_func.calculate_file_crc64(self.filename)
        if self.bucket.object_exists(self.key):
            metastr = self.bucket.head_object(self.key)
            if str(crc64) == metastr.headers['x-oss-hash-crc64ecma']:
                wx.CallAfter(pub.sendMessage, "pgMsg", msg='%s\tSkip' % (self.key))

                return

        if os.path.exists(self.json_file):
            self.Resume_Upfile_Parts(self.json_file, crc64)
            return


        if not os.path.exists("upload"):
            os.makedirs("upload")
        self.Upfile_Parts(self.json_file, crc64, self.key,self.filename)

        # # 验证分片上传。
        # #with open(filename, 'rb') as fileobj:
        # #    assert bucket.get_object(key).read() == fileobj.read()

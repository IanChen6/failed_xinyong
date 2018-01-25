# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     
   Description :
   Author :       ianchen
   date：          
-------------------------------------------------
   Change Activity:
                   2017/11/22:
-------------------------------------------------
"""
import base64
import hashlib
import json
import logging
import random
import time
import pymssql
import os
import redis
import requests
from lxml import etree
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.support import ui
from get_db import get_db, job_finish
import sys
from log_ging.log_01 import create_logger
from urllib.parse import quote

class szcredit(object):
    def __init__(self, cn, sID, batchid, companyid, customerid, logger):
        self.headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Host': 'www.szcredit.org.cn',
                        'Cookie': 'UM_distinctid=160a1f738438cb-047baf52e99fc4-e323462-232800-160a1f73844679; ASP.NET_SessionId=4bxqhcptbvetxqintxwgshll',
                        'Origin': 'https://www.szcredit.org.cn',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://www.szcredit.org.cn/web/gspt/newGSPTList.aspx?keyword=%u534E%u88D4&codeR=28',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
                        'X-Requested-With': 'XMLHttpRequest',
                        }
        self.logger = logger
        self.batchid = batchid
        self.cn = cn
        self.sID = sID
        self.companyid = companyid
        self.customerid = customerid
        self.query = [sID, cn]
        self.host, self.port, self.db = get_db(companyid)

    def insert_db(self, sql, params):
        conn = pymssql.connect(host=self.host, port=self.port, user='Python', password='pl,okmPL<OKM',
                               database=self.db, charset='utf8')
        cur = conn.cursor()
        if not cur:
            raise Exception("数据库连接失败")
        # cur.callproc('[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]', (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
        len(params)
        cur.callproc(sql, params)
        conn.commit()
        cur.close()

    def login(self):
        for t in range(3):
            session = requests.session()
            try:
                session.proxies = sys.argv[1]
            except:
                self.logger.info("未传代理参数，启用本机IP")
            yzm_url = 'https://www.szcredit.org.cn/web/WebPages/Member/CheckCode.aspx?'
            yzm = session.get(url=yzm_url, headers=self.headers)
            for q in self.query:
                # 处理验证码
                if not q.strip():
                    continue
                with open("yzm.jpg", "wb") as f:
                    f.write(yzm.content)
                    f.close()
                with open('yzm.jpg', 'rb') as f:
                    base64_data = str(base64.b64encode(f.read()))
                    base64_data = "data:image/jpg;base64," + base64_data[2:-1]
                    post_data = {"a": 2, "b": base64_data}
                    post_data = json.dumps({"a": 2, "b": base64_data})
                    res = session.post(url="http://39.108.112.203:8002/mycode.ashx", data=post_data)
                    # print(res.text)
                    f.close()
                postdata = {'action': 'GetEntList',
                            'keyword': q,
                            'type': 'query',
                            'ckfull': 'false',
                            'yzmResult': res.text
                            }
                resp1 = session.post(url='https://www.szcredit.org.cn/web/AJax/Ajax.ashx', headers=self.headers,
                                     data=postdata)
                self.logger.info(resp1.text)
                resp = resp1.json()
                try:
                    result = resp['resultlist']
                except Exception as e:
                    self.logger.warn(e)
                    self.logger.info(resp)
                    self.logger.info("网络连接失败")
                    sleep_time = [3, 4, 3.5, 4.5, 3.2, 3.8, 3.1, 3.7, 3.3, 3.6]
                    time.sleep(sleep_time[random.randint(0, 9)])
                    continue
                if resp1 is not None and resp1.status_code == 200 and result:
                    result_dict = result[0]
                    print(result_dict["RecordID"])  # 获取ID
                    detai_url = 'https://www.szcredit.org.cn/web/gspt/newGSPTDetail3.aspx?ID={}'.format(
                        result_dict["RecordID"])
                    detail = session.get(url=detai_url, headers=self.headers, timeout=30)
                    detail.encoding = detail.apparent_encoding
                    root = etree.HTML(detail.text)  # 将request.content 转化为 Element
                    self.parse(root)
                return

    def parse(self, root):
        title = root.xpath('//*[@id="Table31"]//li[@class="current"]')
        t_list = []
        for t in title:
            tt = t.xpath(".//a[1]/text()")
            print(tt[0])
            t_list.append(tt[0])

        tb_list = []
        tb = root.xpath('//*[@id="Table31"]//table')  # 抓取table31
        for i in tb:
            data_json = []
            tb_detail = i.xpath(".//tr")
            for j in tb_detail:
                t = j.xpath('./td//text()')
                data_json.append(t)
                # data_json[t[0]]=t[1]
            # data_json=json.dumps(data_json,ensure_ascii=False)
            # print(data_json)
            tb_list.append(data_json)

        data_dict = {}
        for i in range(len(t_list)):
            data_dict[t_list[i]] = tb_list[i]
        print(data_dict)

        if "登记备案信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["登记备案信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["登记备案信息"] = d1
            # dm = {}
            # dm["登记备案信息"] = d1
            # print(dm)

        if "股东登记信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["股东登记信息"]
            d2 = {}
            for i in get_data[1:]:
                d3 = {}
                d3['出资额'] = i[4]
                d3['出资比例'] = i[5]
                d2[i[0]] = d3
            d1['股东名称'] = d2
            data_dict["股东登记信息"] = d1
            dm = {}
            dm["股东登记信息"] = d1
            print(dm)

        if "成员登记信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["成员登记信息"]
            for i in get_data[1:]:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["成员登记信息"] = d1
            # dm = {}
            # dm["成员登记信息"] = d1
            # print(dm)

        if "税务登记信息(国税)" in data_dict.keys():
            d1 = {}
            get_data = data_dict["税务登记信息(国税)"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["税务登记信息(国税)"] = d1
            # dm = {}
            # dm["税务登记信息(国税)"] = d1
            # print(dm)

        if "税务登记信息(地税)" in data_dict.keys():
            d1 = {}
            get_data = data_dict["税务登记信息(地税)"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["税务登记信息(地税)"] = d1
            # dm = {}
            # dm["税务登记信息(地税)"] = d1
            # print(dm)

        if "机构代码信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["机构代码信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["机构代码信息"] = d1
            # dm = {}
            # dm["机构代码信息"] = d1
            # print(dm)

        if "印章备案信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["印章备案信息"]
            d2 = {}
            for i in get_data[1:]:
                d3 = {}
                d3['印章编码'] = i[1]
                d3['审批日期'] = i[2]
                d3['备案日期'] = i[3]
                d3['备案情况'] = i[4]
                d3['详情'] = i[5]
                d2[i[0]] = d3
            d1['印章名称'] = d2
            data_dict["印章备案信息"] = d1
            # dm = {}
            # dm["印章备案信息"] = d1
            # print(dm)

        if "企业参保信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["企业参保信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["企业参保信息"] = d1
            # dm = {}
            # dm["企业参保信息"] = d1
            # print(dm)

        if "海关企业基本登记信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["海关企业基本登记信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["海关企业基本登记信息"] = d1
            # dm = {}
            # dm["海关企业基本登记信息"] = d1
            # print(dm)

        if "高新技术企业认定信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["高新技术企业认定信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["高新技术企业认定信息"] = d1
            # dm = {}
            # dm["高新技术企业认定信息"] = d1
            # print(dm)

        if "对外贸易经营者备案登记资料" in data_dict.keys():
            d1 = {}
            get_data = data_dict["对外贸易经营者备案登记资料"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["对外贸易经营者备案登记资料"] = d1
            # dm = {}
            # dm["对外贸易经营者备案登记资料"] = d1
            # print(dm)

        if "住房公积金缴存数据表" in data_dict.keys():
            d1 = {}
            get_data = data_dict["住房公积金缴存数据表"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["住房公积金缴存数据表"] = d1
            # dm = {}
            # dm["住房公积金缴存数据表"] = d1
            # print(dm)

        if "电子商务认证企业信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["电子商务认证企业信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["电子商务认证企业信息"] = d1
            # dm = {}
            # dm["电子商务认证企业信息"] = d1
            # print(dm)

        if "电子商务认证企业网站信息" in data_dict.keys():
            d1 = {}
            get_data = data_dict["电子商务认证企业网站信息"]
            for i in get_data:
                try:
                    d1[i[0]] = i[1]
                except:
                    d1[i[0]] = ""
            data_dict["电子商务认证企业网站信息"] = d1
            # dm = {}
            # dm["电子商务认证企业网站信息"] = d1
            # print(dm)

        if "企业年报信息" in data_dict.keys():
            get_data = data_dict["企业年报信息"]
            d2 = {}
            for i in range(int(len(get_data) / 2)):
                d3 = {}
                d3['报送年度'] = get_data[i * 2][1]
                d3['发布日期'] = get_data[i * 2 + 1][1]
                d2[i + 1] = d3
            data_dict["企业年报信息"] = d1
            # dm = {}
            # dm["企业年报信息"] = d2
            # print(dm)

        # 企业变更信息
        try:
            title = root.xpath('//*[@id="Table123"]//li[@class="current"]')
            t_list = []
            for t in title:
                tt = t.xpath("./text()")
                print(tt[0])
                t_list.append(tt[0])

            tb_list = []
            tb = root.xpath('//*[@id="Table123"]//table')  # 抓取table31

            for i in tb:
                data_json = []
                tb_detail = i.xpath(".//tr")
                for j in tb_detail:
                    t = j.xpath('./td//text()')
                    data_json.append(t)
                    # data_json[t[0]]=t[1]
                # data_json=json.dumps(data_json,ensure_ascii=False)
                # print(data_json)
                tb_list.append(data_json)

            for i in range(len(t_list)):
                data_dict[t_list[i]] = tb_list[i]

            if "企业变更信息" in data_dict.keys():
                d1 = {}
                get_data = data_dict["企业变更信息"]
                d2 = {}

                for i in get_data[1:]:
                    d2['变更日期'] = i[1]
                    d2['变更事项'] = i[2]
                    d1[i[0]] = d2
                data_dict["企业变更信息"] = d1
        except:
            print("No exist")

        all_urls = []
        all_gd = []
        gdjg = {}
        gdxx = root.xpath('//*[@id="tb_1"]//tr')
        for i in gdxx[1:]:
            lianjie = i.xpath('.//@href')[0]
            lianjie = lianjie.strip()
            gdm = i.xpath('./td[1]/text()')[0]
            print(lianjie)
            all_urls.append(lianjie)
            all_gd.append(gdm)
        for j in range(len(all_urls)):
            clean_dict = {}
            gd_url = "https://www.szcredit.org.cn/web/gspt/{}".format(all_urls[j])
            gd_resp = requests.get(url=gd_url, headers=self.headers)
            gd_resp.encoding = gd_resp.apparent_encoding
            root = etree.HTML(gd_resp.text)
            gdxq = root.xpath('//table[@class="list"]//tr')
            a = 1
            for xq in gdxq[1:]:
                sb = {}
                xx = xq.xpath('.//text()')
                clean = []
                for s in xx:
                    s = s.strip()
                    if s.strip and s is not "":
                        clean.append(s)
                print(clean)
                sb["企业名称"] = clean[0]
                sb["企业注册号"] = clean[1]
                sb["企业类型"] = clean[2]
                sb["成立日期"] = clean[3]
                clean_dict["{}".format(a)] = sb
                a += 1
            gdjg[all_gd[j]] = clean_dict
        print(gdjg)

        print(data_dict)
        data_dict["关联公司信息"] = gdjg
        infojson = json.dumps(data_dict, ensure_ascii=False)
        self.logger.info(infojson)
        params = (
            self.batchid, self.companyid, self.customerid, self.cn, self.sID, infojson
        )
        self.insert_db("[dbo].[Python_Serivce_WXWebShenZhen_Add]", params)

    def ssdjp(self):
        ip = ['121.31.159.197', '175.30.238.78', '124.202.247.110']
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://app02.szmqs.gov.cn',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'x-form-id': 'mobile-signin-form',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://app02.szmqs.gov.cn/outer/entSelect/gs.html',
            'X-Forwarded-For': ip[random.randint(0, 2)]
            # 'Cookie': 'Hm_lvt_5a517db11da5b1952c8edc36c230a5d6=1516416114; Hm_lpvt_5a517db11da5b1952c8edc36c230a5d6=1516416114; JSESSIONID=0000H--QDbjRJc2YKjpIYc_K3bw:-1'
        }
        session = requests.session()
        try:
            session.proxies = sys.argv[1]
        except:
            self.logger.info("未传代理参数，启用本机IP")
        # name='unifsocicrediden=&entname={}&flag=1'
        # postdata='unifsocicrediden=&entname={}&flag=1'.format()
        s = self.sID
        if s.strip():
            print('not null')
            postdata = 'unifsocicrediden={}&entname=&flag=1'.format(s)
            resp = session.post('https://app02.szmqs.gov.cn/outer/entEnt/detail.do', headers=headers, data=postdata,
                                timeout=30)
            self.logger.info(resp.text)
            gswsj = resp.json()
            gswsj = gswsj['data']
            gswsj = gswsj[0]
            gswsj = gswsj['data']
            jbxx = gswsj[0]
            if 'opto' in jbxx.keys():
                if jbxx['opto'] == "5000-01-01" or jbxx['opto'] == "1900-01-01" or jbxx['opto'].strip():
                    jbxx['营业期限'] = "永续经营"
                else:
                    jbxx['营业期限'] = "自" + jbxx['opfrom'] + "起至" + jbxx['opto'] + "止"
            else:
                jbxx['营业期限'] = "永续经营"

            index_dict = gswsj[0]
            id = index_dict['id']
            regno = index_dict['regno']
            opetype = index_dict['opetype']
            unifsocicrediden = index_dict['unifsocicrediden']
            pripid = index_dict['entflag']
            header2 = {
                'Origin': 'https://app02.szmqs.gov.cn',
                # 'Cookie': 'Hm_lvt_5a517db11da5b1952c8edc36c230a5d6=1516416114,1516590080; Hm_lpvt_5a517db11da5b1952c8edc36c230a5d6=1516590080; JSESSIONID=0000CgpyMFWxBHU8MWpcnjFhHx6:-1',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': 'https://app02.szmqs.gov.cn/outer/entSelect/gs.html',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive'
            }
            xqlist = ['许可经营信息',
                      '股东信息',
                      '成员信息',
                      '变更信息',
                      '股权质押信息',
                      '动产抵押信息',
                      '法院冻结信息',
                      '经营异常信息',
                      '严重违法失信信息']
            tagid = 1
            djxx = {}
            postdata = 'pripid={}&opetype={}'.format(pripid, opetype)
            nbresp = requests.post('https://app02.szmqs.gov.cn/outer/entEnt/nb.do', headers=header2, data=postdata)
            if nbresp.status_code == 200:
                nb = nbresp.json()
                nb = nb['data']
                nb = nb[0]
                nb = nb['data']
                if len(nb) != 0:
                    yearnb = ''
                    for n in nb:
                        yearnb += "" + n['ancheyear'] + "年报已公示、"
                else:
                    yearnb = "无年报信息"
            jbxx["年报情况"] = yearnb
            djxx["基本信息"] = jbxx

            for i in xqlist:
                postdata = 'flag=1&tagId={}&id={}&regno={}&unifsocicrediden={}&opetype={}'.format(tagid, id, regno,
                                                                                                  unifsocicrediden,
                                                                                                  opetype)
                dtresp = requests.post('https://app02.szmqs.gov.cn/outer/entEnt/tag.do', headers=header2, data=postdata)
                if dtresp.status_code == 200:
                    dt = dtresp.json()
                    dt = dt['data']
                    dt = dt[0]
                    dt = dt['data']
                    djxx[i] = dt
                tagid += 1
            djxx = json.dumps(djxx, ensure_ascii=False)
            params = (self.batchid, self.companyid, self.customerid, self.cn, self.sID, djxx)
            self.logger.info(params)
            self.insert_db('[dbo].[Python_Serivce_GSWebShenZhen_Add]', params)
        else:
            name = self.cn
            urlname = quote(name)
            postdata = 'unifsocicrediden=&entname={}&flag=1'.format(urlname)
            resp = session.post('https://app02.szmqs.gov.cn/outer/entEnt/detail.do', headers=headers, data=postdata)
            self.logger.info(resp.text)
            gswsj = resp.json()
            gswsj = gswsj['data']
            gswsj = gswsj[0]
            gswsj = gswsj['data']
            jbxx = gswsj[0]
            if 'opto' in jbxx.keys():
                if jbxx['opto'] == "5000-01-01" or jbxx['opto'] == "1900-01-01" or jbxx['opto'].strip():
                    jbxx['营业期限'] = "永续经营"
                else:
                    jbxx['营业期限'] = "自" + jbxx['opfrom'] + "起至" + jbxx['opto'] + "止"
            else:
                jbxx['营业期限'] = "永续经营"

            index_dict = gswsj[0]
            id = index_dict['id']
            regno = index_dict['regno']
            opetype = index_dict['opetype']
            unifsocicrediden = index_dict['unifsocicrediden']
            pripid = index_dict['entflag']
            header2 = {
                'Origin': 'https://app02.szmqs.gov.cn',
                # 'Cookie': 'Hm_lvt_5a517db11da5b1952c8edc36c230a5d6=1516416114,1516590080; Hm_lpvt_5a517db11da5b1952c8edc36c230a5d6=1516590080; JSESSIONID=0000CgpyMFWxBHU8MWpcnjFhHx6:-1',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': 'https://app02.szmqs.gov.cn/outer/entSelect/gs.html',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive'
            }
            xqlist = ['许可经营信息',
                      '股东信息',
                      '成员信息',
                      '变更信息',
                      '股权质押信息',
                      '动产抵押信息',
                      '法院冻结信息',
                      '经营异常信息',
                      '严重违法失信信息']
            tagid = 1
            djxx = {}
            postdata = 'pripid={}&opetype={}'.format(pripid, opetype)
            nbresp = requests.post('https://app02.szmqs.gov.cn/outer/entEnt/nb.do', headers=header2, data=postdata)
            if nbresp.status_code == 200:
                nb = nbresp.json()
                nb = nb['data']
                nb = nb[0]
                nb = nb['data']
                if len(nb) != 0:
                    yearnb = ''
                    for n in nb:
                        yearnb += "" + n['ancheyear'] + "年报已公示、"
                else:
                    yearnb = "无年报信息"
            jbxx["年报情况"] = yearnb
            djxx["基本信息"] = jbxx

            for i in xqlist:
                postdata = 'flag=1&tagId={}&id={}&regno={}&unifsocicrediden={}&opetype={}'.format(tagid, id, regno,
                                                                                                  unifsocicrediden,
                                                                                                  opetype)
                dtresp = requests.post('https://app02.szmqs.gov.cn/outer/entEnt/tag.do', headers=header2, data=postdata)
                if dtresp.status_code == 200:
                    dt = dtresp.json()
                    dt = dt['data']
                    dt = dt[0]
                    dt = dt['data']
                    djxx[i] = dt
                tagid += 1
            djxx = json.dumps(djxx, ensure_ascii=False)
            params = (self.batchid, self.companyid, self.customerid, self.cn, self.sID, djxx)
            self.logger.info(params)
            self.insert_db('[dbo].[Python_Serivce_GSWebShenZhen_Add]', params)


logger = create_logger(path=os.path.dirname(sys.argv[0]).split('/')[-1])
redis_cli = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)


def run_test(cn, sID, batchid, companyid, customerid):
    print("++++++++++++++++++++++++++++++++++++")
    print('jobs[ts_id=%s] running....' % batchid)
    time.sleep(5)
    try:
        credit = szcredit(cn=cn, sID=sID, batchid=batchid, companyid=companyid, customerid=customerid, logger=logger)
        # try:
        #     credit.ssdjp()
        # except Exception as e:
        #     logger.warn(e)
        #     logger.warn("工商网爬取失败")
        #     goshng_dict = {"1": cn, "2": sID, "3": batchid, "4": companyid,
        #                    "5": customerid, "6": sd["6"], "7": sd["7"], "8": sd["8"]}
        #     pjson = json.dumps(goshng_dict)
        #     redis_cli.lpush("gongshang", pjson)
        try:
            credit.login()
            time.sleep(10)
        except Exception as e:
            logger.info("信用网爬取失败")
            logger.info(e)
            xinyong_dict = {"1": cn, "2": sID, "3": batchid, "4": companyid,
                            "5": customerid, "6": sd["6"], "7": sd["7"], "8": sd["8"]}
            pjson = json.dumps(xinyong_dict)
            redis_cli.lpush("xinyong", pjson)
        logger.info("深圳企业信用网信息抓取完成")
    except Exception as e:
        logger.error(e)
        # job_finish(sd["6"], sd["7"], sd["8"], sd["3"], sd["4"], sd["5"], '-1', 'error')
    print('jobs[ts_id=%s] done' % batchid)
    result = True
    return result


while True:
    # ss=redis_cli.lindex("list",0)
    ss = redis_cli.lpop("xinyong")
    sleep_time = [3, 2, 5, 7, 9, 10, 1, 4, 8, 6]
    time.sleep(sleep_time[random.randint(0, 9)])
    if ss is not None:
        # print(redis_cli.lpop("list"))
        sd = json.loads(ss)
        run_test(sd["1"], sd["2"], sd["3"], sd["4"], sd["5"])
    else:
        time.sleep(10)
        print("no task waited")

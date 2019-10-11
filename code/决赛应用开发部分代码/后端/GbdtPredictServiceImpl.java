package  com.alibaba.dataworks.service.impl.predict;

import  com.alibaba.dataworks.service.predict.GbdtPredictService;
import  com.alibaba.dataworks.service.bo.GbdtPredictBO;
import  org.slf4j.Logger;
import  org.slf4j.LoggerFactory;
import  org.springframework.stereotype.Service;
import  org.springframework.beans.factory.annotation.Value;
import  com.alibaba.dataworks.common.Result;
import  com.aliyun.dataworks.dataservice.sdk.facade.DataApiClient;
import  com.alibaba.dataworks.service.PaiApiService;
import  org.springframework.beans.factory.annotation.Autowired;
import  com.alibaba.cloudapi.sdk.core.model.ApiResponse;
import  com.alibaba.fastjson.JSONArray;
import  com.aliyun.dataworks.dataservice.model.api.protocol.ApiProtocol;
import  com.aliyun.dataworks.dataservice.sdk.loader.http.Request;


import  java.util.ArrayList;
import  java.util.List;
import  java.util.HashMap;
import  java.util.LinkedHashMap;
import  java.util.Map;
import  java.util.Collections;
import  java.util.stream.Collectors;

@Service
public  class  GbdtPredictServiceImpl  implements  GbdtPredictService  {
        Logger  logger  =  LoggerFactory.getLogger(GbdtPredictServiceImpl.class);

        @Value("${ide.proxy.data.service.app-key}")
        String  appKey;

        @Value("${ide.proxy.data.service.app-secret}")
        String  appSecret;

        @Value("${pai.api.token}")
        String  apiToken;

        @Value("${ide.proxy.data.service.host}")
        String  apiGroupHost;

        @Autowired
        PaiApiService  paiApiService;

        @Autowired
        DataApiClient  dataApiClient;

        /**
          *  具体业务处理逻辑
          *  @param  userId
          *  @throws  Exception
          */
        @Override
        public  Result  getGbdtPredict(String  userId)  throws  Exception  {
                //step1:*********调用api服务获取用户特征*********
                Request  request  =  new  Request();
                request.setApiProtocol(ApiProtocol.of(0));
                request.setMethod("GET");
                request.setHost(apiGroupHost);
                request.setPath("/zhaopin_round2_user_get_fea");
                request.setAppKey(appKey);
                request.setAppSecret(appSecret);
                request.setTimeout(10000);
                Map<String  ,  Object>  map  =  new  HashMap<String  ,  Object>();
                //  String  user_id  =  "001cb95e77e42831e935b6cc09776099";
                map.put("user_id",userId);
                request.setQuerys(map);

                HashMap  response  =  dataApiClient.dataLoad(request);
                //(apiPath  ,  features.getBytes());
                //System.out.println(response);

                //step2:*********调用paiApi  进行预测*********
                String  apiPath  =  "/EAPI_1576491055843382_zhaopin_round2_azu";
                JSONArray  feaArray  =  (JSONArray)  response.get("data");
                //拿到jd_no  列表
                List<String>  jdNoList  =  new  ArrayList<>();
                for(int  i=0;i<feaArray.size();i++){
                        jdNoList.add(feaArray.getJSONObject(i).get("jd_no").toString());
                }
                //  System.out.println(jdNoList);

                String  features  =  response.get("data").toString();
                ApiResponse  response2  =  paiApiService.syncPredict(apiPath,  features.getBytes());
                String scoreString = new String(response2.getBody());
                JSONArray scoreArray = JSONArray.parseArray(scoreString);
                //拿到score
                LinkedHashMap<String,Double> result = new LinkedHashMap<>();
                for(int i=0;i<jdNoList.size();i++){
                    result.put(jdNoList.get(i), Double.parseDouble(scoreArray.getJSONObject(i).get("p_1").toString()));
                }
                //System.out.println(result);
                //排序
                List<String> sortedResult = result.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).map(e -> e.getKey()).
                        collect(Collectors.toList());
                //截断top50个,避免请求url过长
                sortedResult = sortedResult.subList(0,Math.min(50,sortedResult.size()));
                //step3:*********数据补全*********
                Request request2 = new Request();
                request2.setApiProtocol(ApiProtocol.of(0));
                request2.setMethod("GET");
                request2.setHost(apiGroupHost);
                request2.setPath("/zhaopin_round2_jd_detail2");
                request2.setAppKey(appKey);
                request2.setAppSecret(appSecret);
                request2.setTimeout(10000);
                Map<String , Object> map2 = new HashMap<String , Object>();

                map2.put("jd_no", String.join(",",sortedResult));
                request2.setQuerys(map2);

                HashMap response3 = dataApiClient.dataLoad(request2);
                JSONArray jdDetailArray = (JSONArray) response3.get("data");

                LinkedHashMap<GbdtPredictBO,Double> finalResultTmp = new LinkedHashMap<>();
                for(int i=0;i<jdDetailArray.size();i++){
                    GbdtPredictBO GbdtPredictBOTmp = new GbdtPredictBO();
                    GbdtPredictBOTmp.setJdNo(jdDetailArray.getJSONObject(i).get("jd_no").toString());
                    GbdtPredictBOTmp.setRequireNum(jdDetailArray.getJSONObject(i).get("require_nums").toString());
                    GbdtPredictBOTmp.setMinYears(jdDetailArray.getJSONObject(i).get("min_years").toString());
                    GbdtPredictBOTmp.setMinEdu(jdDetailArray.getJSONObject(i).get("min_edu_level").toString());
                    GbdtPredictBOTmp.setJdTitle(jdDetailArray.getJSONObject(i).get("jd_title").toString());
                    GbdtPredictBOTmp.setJdType(jdDetailArray.getJSONObject(i).get("jd_sub_type").toString());
                    GbdtPredictBOTmp.setPublishDay(jdDetailArray.getJSONObject(i).get("start_date").toString());
                    GbdtPredictBOTmp.setEndDay(jdDetailArray.getJSONObject(i).get("end_date").toString());
                    GbdtPredictBOTmp.setJdCity(jdDetailArray.getJSONObject(i).get("city").toString());
                    GbdtPredictBOTmp.setJdDesc(jdDetailArray.getJSONObject(i).get("job_description").toString());
                    GbdtPredictBOTmp.setSalary(jdDetailArray.getJSONObject(i).get("salary").toString());
                    GbdtPredictBOTmp.setScore(result.get(jdDetailArray.getJSONObject(i).get("jd_no").toString()));
                    finalResultTmp.put(GbdtPredictBOTmp, GbdtPredictBOTmp.getScore());
                }
                //排序
                List<GbdtPredictBO> finalResult = finalResultTmp.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).map(e -> e.getKey()).
                        collect(Collectors.toList());
                return Result.ofSuccess(finalResult);
    }
}
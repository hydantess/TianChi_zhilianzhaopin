package com.alibaba.dataworks.controller.api.predict;

import com.alibaba.dataworks.service.predict.GbdtPredictService;
import com.aliyun.dataworks.dataservice.sdk.facade.DataApiClient;
import com.alibaba.dataworks.service.PaiApiService;
import com.alibaba.dataworks.service.bo.GbdtPredictBO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.GetMapping;
import com.alibaba.dataworks.common.Result;

import java.util.List;

/**
 * GbdtPredict Api Controller
 */
@Controller
public class GbdtPredictApiController {

    private Logger logger = LoggerFactory.getLogger(GbdtPredictApiController.class) ;

    @Autowired
    GbdtPredictService gbdtPredictService;

    @Autowired
    PaiApiService paiApiService;

    @Autowired
    DataApiClient dataApiClient;

    @GetMapping(value = "/GbdtPredict")
    @ResponseBody
    public Result gbdtPredict(@RequestParam(name = "userId", required = true, defaultValue = "001cb95e77e42831e935b6cc09776099") String userId) throws Exception{

        return gbdtPredictService.getGbdtPredict(userId);
    }
}

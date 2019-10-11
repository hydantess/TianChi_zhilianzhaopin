package com.alibaba.dataworks.service.predict;
import java.util.List;
import com.alibaba.dataworks.common.Result;

public interface GbdtPredictService {

    /**
     * 具体业务处理逻辑
     * @param userId
     */
    Result getGbdtPredict(String userId) throws Exception;
}

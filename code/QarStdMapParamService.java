package com.reda.pmd.web.service;


import com.alibaba.druid.util.Base64;
import com.alibaba.fastjson.JSON;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.google.common.base.Preconditions;
import com.reda.framework.model.DubboResp;
import com.reda.framework.model.PageEntity;
import com.reda.framework.utils.T;
import com.reda.pmd.common.exception.ServiceException;
import com.reda.pmd.web.cacheUtils.PmdCacheUtil;
import com.reda.pmd.web.constant.*;
import com.reda.pmd.web.dao.*;
import com.reda.pmd.web.entity.*;
import com.reda.pmd.web.utils.ClassCompareUtil;
import com.reda.pmd.web.utils.ExcelUtil;
import com.reda.pmd.web.utils.RdAdminDecryptUtil;
import com.reda.pmd.web.vo.DownloadFileVo;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.reda.pmd.web.constant.SplitConst.SPLIT_VERTICAL_BAR_ORIGIN;

/**
 * 功能概述：
 * <p>
 * <p>Date: 2020-09-15 14:09
 * <p>Copyright: Copyright(c)2020 RedaFlight.com All Rights Reserved
 *
 * @author Songchuan
 * @version 1.0
 * @since JDK 1.8(231)
 */
@Service
public class QarStdMapParamService {

    @Autowired
    private QarStdMapParamDao mapParamDao;
    @Autowired
    private QarStdMapCodeDao mapCodeDao;
    @Autowired
    private QarStdFapParamDao fapParamDao;
    @Autowired
    private QarStdCodeRelationDao codeRelationDao;
    @Autowired
    private QarStdFapParamService fapParamService;
    @Autowired
    private QarStdVersionHistoryDao qarStdVersionHistoryDao;
    @Autowired
    private QarStdFapMapRelationDao fapMapRelationDao;
    @Autowired
    private QarStdMapQualityRelationDao qarStdMapQualityRelationDao;
    @Autowired
    private QarStdRomDependMapDao qarStdRomDependMapDao;
    @Autowired
    private QarStdSnapshotMapRelationDao snapshotMapRelationDao;
    @Autowired
    private QarStdTemplateMapRelationDao templateMapRelationDao;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 分页查询
     *
     * @param pageEntity
     * @return
     */
    public PageEntity<List<QarStdMapParam>> findList(PageEntity<QarStdMapParam> pageEntity) {
        Page<QarStdMapParam> page = PageHelper.startPage(pageEntity == null ? 1 : pageEntity.getPageNum(), pageEntity == null ? 10 : pageEntity.getPageSize()).doSelectPage(
                () -> {
                    QarStdMapParam record = pageEntity.getData();
                    if (record != null){
                        record.setOrderByAndSortBy(pageEntity.getOrderBy());
                        record.setFapCount(PmdCacheUtil.getFapAll().size());
                    }
                    mapParamDao.findList(record);
                });
        List<QarStdMapParam> result = page.getResult();
        if (RdAdminDecryptUtil.isFactoryLock()){
            result.forEach(e->e.setEditTag("1"));
        }else{
            //如果是其他用户，就需要根据字段判断，属于厂家定义的0，不能编辑；属于用户定义的1，可编辑
            result.forEach(e->{
                //设置是否可编辑
                String factoryLock = e.getFactoryLock();
                e.setEditTag(T.StringUtils.isNotEmpty(factoryLock)?factoryLock:"0");
            });
        }

        return new PageEntity(page.getPageNum(), page.getPageSize(), page.getTotal(), page.getResult());
    }

    /**
     * 新增
     *
     * @param qarStdMapParam
     * @return
     */
    public void insert(QarStdMapParam qarStdMapParam) {
        //1.新增参数
        qarStdMapParam.preInsert();
        qarStdMapParam.setEnableStatus(PmdConst.ENABLE_STATUS_CLOSE);
        mapParamDao.insert(qarStdMapParam);
        PmdCacheUtil.refreshMapParamTableCache();
        //2.新增状态码
        insertStatusCode(qarStdMapParam);
    }

    /**
     * 新增或者状态码
     */
    private void insertStatusCode(QarStdMapParam qarStdMapParam) {
        List<QarStdMapCode> statusCodeList = qarStdMapParam.getStatusCodeList();
        Integer pk = qarStdMapParam.getPk();
        if (pk !=null && T.CollectionUtils.isNotEmpty(statusCodeList)) {
            statusCodeList.stream().forEach(code -> {
                code.preInsert();
                code.setStdMapParamPk(pk);
            });
            mapCodeDao.batchInsertUpdate(statusCodeList);
            PmdCacheUtil.refreshMapCodeTableCache();
        }
    }

    /**
     * 查看
     *
     * @param pk
     * @return
     */
    public QarStdMapParam get(Integer pk) {
        //1.获取参数
        QarStdMapParam vo = PmdCacheUtil.getMapParamByPk(pk);
        //2.获取状态码
        if (vo != null) {
            vo.setStatusCodeList(PmdCacheUtil.getCodeByMapParamPk(pk));
        }
        return vo;
    }

    /**
     * 修改
     * 未发布状态：删除并新增状态码
     * 已发布状态：只可新增状态码
     *
     * @param mapParam
     * @return
     */
    @Transactional(rollbackFor = ServiceException.class)
    public DubboResp update(QarStdMapParam mapParam) throws ServiceException {
        //1.修改参数
        List<QarStdMapParam> mapParamAll = PmdCacheUtil.getMapParamAll();
        List<QarStdMapParam> mapParamAllByRef = mapParamAll.stream().filter(e -> Objects.equals(e.getRefNumber(), mapParam.getRefNumber())).collect(Collectors.toList());
        List<QarStdMapParam> otherMapParam = mapParamAllByRef.stream().filter(e -> !Objects.equals(e.getPk(), mapParam.getPk())).collect(Collectors.toList());
        if(T.CollectionUtils.isNotEmpty(otherMapParam)){
            return T.DubboRespUtils.buildFailResp("编号已存在，请修改");
        }
        mapParam.preUpdate();
        //2.更新，如果是修改操作，不是修改启用状态就需要更新版本，不是更新操作不往下执行
        if (!mapParam.isUpdate()) {
            mapParamDao.update(mapParam);
            PmdCacheUtil.refreshMapParamTableCache();
            return T.DubboRespUtils.buildSuccResp("修改成功");
        }
        //2.1 判断状态码长度限制
        List<QarStdMapCode> statusCodeList = mapParam.getStatusCodeList();
        StringBuilder sbError = new StringBuilder();
        statusCodeList.stream().forEach(e->{
            String codeValue = e.getCodeValue();
            String codeDefine = e.getCodeDefine();
            StringBuilder sbErrorByCode = new StringBuilder();
            if (!T.StringUtils.isNumeric(codeValue)){
                sbErrorByCode.append("不是数字,");
            }
            if (T.StringUtils.isNotEmpty(codeValue) && codeValue.length()>20){
                sbErrorByCode.append("长度不能大于20,");
            }
            if (T.StringUtils.isNotEmpty(codeDefine) && codeDefine.length()>50){
                sbErrorByCode.append("含义长度不能大于50,");
            }
            if (T.StringUtils.isNotEmpty(sbErrorByCode)){
                sbError.append("[状态码"+codeValue+"]:"+sbErrorByCode);
            }
        });
        if (T.StringUtils.isNotEmpty(sbError)){
            return T.DubboRespUtils.buildFailResp(sbError.toString());
        }
        //3.判断版本历史记录表
        QarStdMapParam oldMapParam = mapParamAllByRef.get(0);
        //3.1比较普通字段
        Map<String, Map<String, Object>> map1 = ClassCompareUtil.compareFields(oldMapParam, mapParam);
        //3.2比较长文本字段
        Map<String, Map<String, Object>> map2 = ClassCompareUtil.compareFields(oldMapParam, mapParam);
        if (map1.size() > 0 || map2.size() > 0) {
            String modifyDescription = getBaseDetail(map1, "<br/>");
            //如果核心字段不为空就增加版本记录
            if(T.StringUtils.isNotEmpty(modifyDescription)){
                Integer oldVersion = oldMapParam.getVersion();
                mapParam.setVersion(oldVersion == null?1:++oldVersion);
                insertRecord(mapParam, PmdConst.ROM_MOFIFY_TYPE_2, modifyDescription, getDescription(map2, "<br/>"));
            }
        }
        //3.3、更新map
        mapParamDao.update(mapParam);
        //4、删除被删除的，

        List<Integer> updatePk = statusCodeList.stream().filter(e -> e.getPk() != null).map(QarStdMapCode::getPk).collect(Collectors.toList());
        Integer mapParamPk = mapParam.getPk();
        List<QarStdMapCode> codeByMapParamPk = PmdCacheUtil.getCodeByMapParamPk(mapParamPk);
        List<Integer> dbAllPk = codeByMapParamPk.stream().filter(e -> e.getPk() != null).map(QarStdMapCode::getPk).collect(Collectors.toList());
        dbAllPk.removeAll(updatePk);
        //5、删除掉删除的qar_std_code_relation
        if (T.CollectionUtils.isNotEmpty(dbAllPk)){
            mapCodeDao.deleteByPkList(dbAllPk);
            codeRelationDao.deleteByStdMapCodePk(dbAllPk);
        }
        //6.并新增更新状态码
        insertStatusCode(mapParam);
        PmdCacheUtil.refreshMapParamTableCache();
        PmdCacheUtil.refreshMapCodeTableCache();
        return T.DubboRespUtils.buildSuccResp("修改成功");

    }

    /**
     * 删除
     *
     * @param qarStdMapParam
     * @return
     */
    @Transactional(rollbackFor = ServiceException.class)
    public void delete(QarStdMapParam qarStdMapParam) throws ServiceException{
        List<Integer> pkList = qarStdMapParam.getPkList();
        //2.删除状态码
        mapCodeDao.deleteByMapParamPkList(pkList);
        PmdCacheUtil.refreshMapCodeTableCache();
        //3.删除qar_std_fap_map_relation译码库与标准参数映射关系
        fapMapRelationDao.deleteByStdMapParamPkList(pkList);
        //4、qar_std_map_quality_relation质检参数（测量参数关联质检模型关系表）
        qarStdMapQualityRelationDao.deleteByMapParamPkList(pkList);
        //5、qar_std_rom_depend_map测量参数依赖标准参数表
        qarStdRomDependMapDao.deleteByStdMapParamPkList(pkList);
        //6、qar_std_snapshot_map_relation快照-标准参数关系表
        snapshotMapRelationDao.deleteByStdMapParamPkList(pkList);
        //7、qar_std_template_map_relation工程值模板-标准参数关系表
        templateMapRelationDao.deleteByStdMapParamPkList(pkList);
        //8、记录到操作表
        pkList.stream().forEach(e->{
            QarStdMapParam mapParamByPk = PmdCacheUtil.getMapParamByPk(e);
            insertRecord(mapParamByPk, PmdConst.ROM_MOFIFY_TYPE_3, "删除！", null);
        });
        //1、删除标准参数--最后删除
        mapParamDao.batchDelete(pkList);

        PmdCacheUtil.refreshMapParamTableCache();

    }

    /**
     * 获取修改关键字段
     *
     * @param map
     * @return
     */
    private String getBaseDetail(Map<String, Map<String, Object>> map, String br) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Map<String, Object>> entry : map.entrySet()) {
            String baseParamName = QarStdMapParam.getBaseParamName(entry.getKey());
            if(T.StringUtils.isNotEmpty(baseParamName)){
                sb.append(baseParamName).append(": ");
                Map<String, Object> value = entry.getValue();
                sb.append(value.get("oldValue")).append(" → ").append(value.get("newValue")).append(";").append(br);
            }
        }
        return sb.toString();
    }

    /**
     * 获取修改内容文字描述
     *
     * @param map
     * @return
     */
    private String getDescription(Map<String, Map<String, Object>> map, String br) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Map<String, Object>> entry : map.entrySet()) {
            String detailParamName = QarStdMapParam.getDetailParamName(entry.getKey());
            if(T.StringUtils.isNotEmpty(detailParamName)){
                sb.append(detailParamName).append(": ");
                Map<String, Object> value = entry.getValue();
                sb.append(value.get("oldValue")).append(" → ").append(value.get("newValue")).append(";").append(br);
            }
        }
        return sb.toString();
    }

    /**
     * 手动匹配页面-获取构型参数和标准参数
     * stdFapPk：译码库PK
     * stdMapParamPk:标准参数PK
     * stdFapParamPk:构型参数PK
     *
     * @param relation
     * @return
     */
    public Map<String, Object> getFapParam(QarStdFapMapRelation relation) {
        Map<String, Object> map = new HashMap<>();
        Integer stdFapPk = relation.getStdFapPk();
        Integer stdFapParamPk = relation.getStdFapParamPk();
        Integer stdMapParamPk = relation.getStdMapParamPk();
        //1.根据译码库获取构型参数
        PageEntity<QarStdFapParam> pageEntity = new PageEntity<>();
        QarStdFapParam data = new QarStdFapParam();
        data.setStdFapPk(stdFapPk);
        pageEntity.setData(data);
        List<QarStdFapParam> fapParamList = findFapParamListManual(pageEntity).getData();
        if (stdFapParamPk != null
                && !fapParamList.stream().filter(vo -> vo.getPk().equals(stdFapParamPk)).findAny().isPresent()) {
            //加一个默认选择的构型参数,如果该参数不在fapParamList里的话
            fapParamList.add(0, fapParamService.get(new QarStdFapParam(stdFapParamPk)));
        }
        map.put("fapParamList", fapParamList);
        //2.当前列的标准参数的详细属性
        map.put("qarStdMap", get(stdMapParamPk));
        //3.状态码 关联关系
        if (stdFapParamPk != null) {
            map.put("codeRelationList", codeRelationDao.getCodeRelationList(relation));
        } else {
            map.put("codeRelationList",Collections.emptyList());
        }
        return map;
    }
    /**
     * 新增操作记录-只有版本变化才加
     *
     * @param qarStdMapParam
     */
    private void insertRecord(QarStdMapParam qarStdMapParam, String modifyType, String modifyDescription, String modifyDetail) {
        QarStdMapParam mapParam = PmdCacheUtil.getMapParamByPk(qarStdMapParam.getPk());
        String mnemonic = mapParam.getMnemonic();
        List<QarStdVersionHistory> qarStdRecordList = qarStdVersionHistoryDao.getQarStdRecordList(mnemonic, EnumVersionHistoryDataType.TYPE_MAP_PARAM.getCode());
        int maxHistoryVersion =1;
        if (T.CollectionUtils.isNotEmpty(qarStdRecordList)){
         maxHistoryVersion = qarStdRecordList.stream().mapToInt(QarStdVersionHistory::getVersion).max().getAsInt();
        }
        QarStdVersionHistory record = new QarStdVersionHistory();
        record.preInsert();
        record.setDataType(EnumVersionHistoryDataType.TYPE_MAP_PARAM.getCode());
        record.setCode(mnemonic);
        record.setVersion(++maxHistoryVersion);
        record.setJsonData(JSON.toJSON(qarStdMapParam).toString());
        record.setModifyType(modifyType);
        record.setModifyDescription(modifyDescription);
        record.setModifyDetail(modifyDetail);
        qarStdVersionHistoryDao.insert(record);
    }

    /**
     * 手动匹配页面 查询构型参数
     *
     * @return
     */
    public PageEntity<List<QarStdFapParam>> findFapParamListManual(PageEntity<QarStdFapParam> pageEntity) {
        Page<QarStdFapParam> page = PageHelper.startPage(pageEntity == null ? 1 : pageEntity.getPageNum(), pageEntity == null ? 10 : pageEntity.getPageSize()).doSelectPage(
                () -> {
                    QarStdFapParam record = pageEntity.getData();
                    record.setOrderByAndSortBy(pageEntity.getOrderBy());
                     List<QarStdFapParam> fapParamList = fapParamDao.findFapParamListManual(record);
                    //获取状态码
                    fapParamList.stream().forEach(vo -> vo.setStatusCodeList(PmdCacheUtil.getCodeByFapParamPk(vo.getPk())));
                });
        return new PageEntity(page.getPageNum(), page.getPageSize(), page.getTotal(), page.getResult());
    }

    /**
     * 导出标准参数
     *
     * @return
     */
    public DownloadFileVo exportMapParam(QarStdMapParam qarStdMapParam) throws IOException {
        String filename = "标准参数.xlsx";
        // 从缓存中获取标准参数
        List<QarStdMapParam> mapParamList = mapParamDao.findList(qarStdMapParam);
        Preconditions.checkArgument(T.CollectionUtils.isNotEmpty(mapParamList), "标准参数没有数据");

        mapParamList.forEach(mapParam -> {
                    // 填充状态码
                    mapParam.setStatusCodeList(PmdCacheUtil.getCodeByMapParamPk(mapParam.getPk()));
                    // 类型
                    Optional.ofNullable(mapParam.getType()).ifPresent(e -> {
                        String typeName = MapParamTypeEnum.getNameByCode(e);
                        mapParam.setType(e + SplitConst.SPLIT_COLON + typeName);
                    });
                }
        );

        // 根据助记码排序
        mapParamList.sort(Comparator.comparing(QarStdMapParam::getMnemonic));

        //创建表格
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook();
        XSSFSheet sheet = xssfWorkbook.createSheet();
        //创建表头
        XSSFRow headRow = sheet.createRow(0);

        // 添加表头
        int headerIndex = 0;
        for (String header : TABLE_HEADER) {
            headRow.createCell(headerIndex++).setCellValue(header);
        }

        // 表内容
        int size = mapParamList.size();
        for (int i = 0; i < size; i++) {
            //创建一行
            XSSFRow row = sheet.createRow(i + 1);

            QarStdMapParam mapParam = mapParamList.get(i);
            headerIndex = 0;
            row.createCell(headerIndex++).setCellValue(mapParam.getMnemonic());
            row.createCell(headerIndex++).setCellValue(mapParam.getDescription());
            row.createCell(headerIndex++).setCellValue(mapParam.getUnit());
            row.createCell(headerIndex++).setCellValue(mapParam.getType());
            // 状态码拼接，格式如：1:A|2:B
            List<QarStdMapCode> statusCodeList = mapParam.getStatusCodeList();
            if (T.CollectionUtils.isNotEmpty(statusCodeList)) {
                String codeStr = statusCodeList.stream()
                        .map(code -> code.getCodeValue() + SplitConst.SPLIT_COLON + code.getCodeDefine())
                        .collect(Collectors.joining(SPLIT_VERTICAL_BAR_ORIGIN));
                row.createCell(headerIndex++).setCellValue(codeStr);
            }
            // 转换关系放空值
            row.createCell(headerIndex++).setCellValue("");
        }
        //表头自动长度
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExcelUtil.autoHeadLength(TABLE_HEADER.length,sheet);
        try {
            ExcelUtil.setWatermarkToExcel(xssfWorkbook,sheet);
            xssfWorkbook.write(baos);
            T.IOUtils.closeQuietly(baos);
            byte[] result = baos.toByteArray();
            return new DownloadFileVo(filename, Base64.byteArrayToBase64(result));
        }catch (IOException e){
            T.IOUtils.closeQuietly(baos);
            return new DownloadFileVo(filename, "");
        }
    }

    /**
     * 标准参数导出表头
     */
    private static final String[] TABLE_HEADER = {
            "标准参数",
            "参数描述",
            "单位",
            "类型",
            "状态码",
            "转换关系"
    };

    /** 检测标准参数的完整性
     * @author wuhan
     * @date 2021/6/18
     * @params [vo]
     */
    public Map<Integer,String> checkMapParam(Integer mapPk,List<QarStdFapMapRelation> allRelation) {
        int index = 0;
        Map<Integer, String> resultMap = new HashMap<>();
        QarStdMapParam qarStdMapParam = PmdCacheUtil.getMapParamByPk(mapPk);
        String mnemonic = qarStdMapParam.getMnemonic();
        //2、译码库完整性
        List<QarStdFap> fapAll = PmdCacheUtil.getFapByStatus(PmdConst.ENABLE_STATUS_OPEN);
        int size = fapAll.size();
        qarStdMapParam.setFapCount(size);
        List<QarStdMapParam> list = fapMapRelationDao.mapParamInitForCheck(qarStdMapParam);
        if (T.CollectionUtils.isEmpty(list)){
            List<String> collect = fapAll.stream().map(r -> r.getDecodeName() + "(" + r.getDecodeVer() + ")").collect(Collectors.toList());
            logger.error("检测，"+mnemonic+":标准参数映射译码库不完整"+collect);
            resultMap.put(index++,"标准参数映射译码库"+collect+"未映射");
        }
        if (T.CollectionUtils.isNotEmpty(list)){
            QarStdMapParam mapParam = list.get(0);
            List<String> unRelationFap = new ArrayList<>();
            if(!Objects.equals(mapParam.getMapFapCount(),size)){
                String fapPks = mapParam.getFapPks();
                if(T.StringUtils.isNotBlank(fapPks)){
                    String[] split = fapPks.split(",");
                    List<String> strings = Arrays.asList(split);
                    unRelationFap = fapAll.stream().filter(e -> !strings.contains(String.valueOf(e.getPk()))).map(r->r.getDecodeName()+"("+r.getDecodeVer()+")").collect(Collectors.toList());
                }
                logger.error("检测，"+mnemonic+":标准参数映射译码库不完整"+unRelationFap);
                resultMap.put(index++,"标准参数映射译码库"+unRelationFap+"未映射");
            }
        }
        //4.单位检测
        //4.1 标准参数单位检测
        String paramType = qarStdMapParam.getType();
        String unit = qarStdMapParam.getUnit();
        //4.1.1 检测标准参数单位，需要判断是否为空
        if (T.StringUtils.isEmpty(unit) && (Objects.equals(MapParamTypeEnum.TYPE_1.getCode(),paramType) || Objects.equals(MapParamTypeEnum.TYPE_2.getCode(),paramType) || Objects.equals(MapParamTypeEnum.TYPE_3.getCode(),paramType))){
            logger.error("检测，"+mnemonic+":标准参数映射单位未配置");
            resultMap.put(index++,"检测，"+mnemonic+":标准参数单位未配置");
        }
        //如果译码库都没维护完整，就不往下验证了，状态码是基于译码库
        if (T.CollectionUtils.isNotEmpty(resultMap)){
            return resultMap;
        }
        //3.如果标准参数类型为5状态码，1：检查状态码完整性
        if (Objects.equals(MapParamTypeEnum.TYPE_5.getCode(),paramType)){
            QarStdFapMapRelation qarStdCodeRelation = new QarStdFapMapRelation();
            qarStdCodeRelation.setStdMapParamPk(mapPk);
            Iterator<QarStdFap> iterator = fapAll.iterator();
            while (iterator.hasNext()){
                QarStdFap next = iterator.next();
                Integer fapPk = next.getPk();
                qarStdCodeRelation.setStdFapPk(fapPk);
                List<QarStdFapMapRelation> byFapPkAndMapParamPk = allRelation.stream().filter(e -> Objects.equals(e.getStdFapPk(), fapPk) && Objects.equals(e.getStdMapParamPk(), mapPk)).collect(Collectors.toList());
                for (QarStdFapMapRelation qarStdFapMapRelation : byFapPkAndMapParamPk) {
                    //3.1只留下类型为状态码的,且需要映射的才需要检测
                    if(Objects.equals(PmdConst.FAP_RELATION_STATUS_1,qarStdFapMapRelation.getRelationStatus())){
                        Integer stdFapParamPk = qarStdFapMapRelation.getStdFapParamPk();
                        List<QarStdFapCode> codeByFapParamPk = PmdCacheUtil.getCodeByFapParamPk(stdFapParamPk);
                        int sizeForFapCode = codeByFapParamPk.size();
                        qarStdCodeRelation.setStdFapParamPk(stdFapParamPk);
                        //3.2获取所有构型参数的code TODO 待使用缓存
                        List<QarStdCodeRelation> codeRelationList = codeRelationDao.getCodeRelationList(qarStdCodeRelation);
                        if (codeRelationList.size()<sizeForFapCode){
                            resultMap.put(index++,"检测标准参数["+qarStdMapParam.getMnemonic()+"]对应译码库["+next.getDecodeName()+"("+next.getDecodeVer()+")]状态码未映射</br>");
                        }
                    }
                }
            }
        }
        //4.单位检测
        //4.2 标准参数和构型参数关联关系检测
        Iterator<QarStdFap> iterator = fapAll.iterator();
        ConcurrentHashMap<String, Object> unitMap = new ConcurrentHashMap<>();
        while (iterator.hasNext()){
            QarStdFap next = iterator.next();
            Integer fapPk = next.getPk();
            List<QarStdFapMapRelation> byFapPkAndMapParamPk = allRelation.stream().filter(e -> Objects.equals(e.getStdFapPk(), fapPk) && Objects.equals(e.getStdMapParamPk(), mapPk)).collect(Collectors.toList());
            for (QarStdFapMapRelation qarStdFapMapRelation : byFapPkAndMapParamPk) {
                //4.2.1只留下需要关联的译码库参数
                if(Objects.equals(PmdConst.FAP_RELATION_STATUS_1,qarStdFapMapRelation.getRelationStatus())){
                    Integer stdFapParamPk = qarStdFapMapRelation.getStdFapParamPk();
                    QarStdFapParam fapParamByPk = PmdCacheUtil.getFapParamByPk(stdFapParamPk);
                    String confirmUnit = fapParamByPk.getConfirmUnit();
                    //4.2.2 检测是否配置confirmUnit确认单位，只有当数值类型才有确认单位，需要判断是否为空
                    if (T.StringUtils.isEmpty(confirmUnit) && (Objects.equals(MapParamTypeEnum.TYPE_1.getCode(),paramType) || Objects.equals(MapParamTypeEnum.TYPE_2.getCode(),paramType) || Objects.equals(MapParamTypeEnum.TYPE_3.getCode(),paramType))){
                        logger.error("检测标准参数["+mnemonic+"]映射的译码库["+next.getDecodeName()+"]构型参数:"+next.getDecodeVer()+"未设置确认单位");
                        resultMap.put(index++,"检测标准参数，["+mnemonic+"]映射的译码库["+next.getDecodeName()+"("+next.getDecodeVer()+")]构型参数["+fapParamByPk.getMnemonic()+"]未设置确认单位</br>");
                    }
                    //4.2.3 检测unit是否单位转换中定义
                    if (T.StringUtils.isNotEmpty(unit) && T.StringUtils.isNotEmpty(confirmUnit) && !Objects.equals(unit,PmdConst.MAP_PARAM_UNIT_FLAG) && !Objects.equals(unit,confirmUnit)){
                        String concatUnitAndConfirmUnit = unit.concat(confirmUnit);
                        if (!unitMap.containsKey(concatUnitAndConfirmUnit)){
                            List<QarStdUnitConversion> qarStdUnitConversionByTargetUnit = PmdCacheUtil.getQarStdUnitConversionByTargetUnit(unit);
                            if (T.CollectionUtils.isEmpty(qarStdUnitConversionByTargetUnit)){
                                logger.error("标准参数["+mnemonic+"]的单位["+unit+"]未在单位转换中定义");
                                resultMap.put(index++,"标准参数["+mnemonic+"]的单位["+unit+"]未在单位转换中定义</br>");
                            }
                            unitMap.put(concatUnitAndConfirmUnit,concatUnitAndConfirmUnit);
                        }
                    }
                }
            }
        }
        //5.TODO 逻辑脚本；python脚本的完整性
        return resultMap;
    }
    /**
     * 自动获取参数编号
     */
    public Integer getGenerateRefNumber() throws ServiceException{
        //获取qar_std_map_param表中最大Ref_Number
        Integer refNumber = mapParamDao.getGenerateRefNumber();
        return refNumber == null ? 1 : refNumber + 1;
    }
}

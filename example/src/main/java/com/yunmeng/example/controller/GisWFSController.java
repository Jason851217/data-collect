package com.yunmeng.example.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

@Controller
@RequestMapping(value = "/gis/wfs")
public class GisWFSController {

    @Value("${gis.wfsIp}")
    private String wfsIp;


    @Value("${gis.wfsUrl}")
    private String wfsUrl;

    @Value("${gis.workSpace}")
    private String gisWorkSpace;

    @Value("${gis.workSpaceUrl}")
    private String gisWorkworkSpaceUrl;

    @SuppressWarnings("deprecation")
    @RequestMapping(value = "/reqUrl")
    @ResponseBody
    public void requestWFS(HttpServletRequest request, HttpServletResponse response) {

        try {
            String layerName = request.getParameter("LayerName");
            String reqUrl = wfsIp + "/geoserver/wfs?service=WFS&version=1.1.0&request=GetFeature&typename=" + layerName + "&outputFormat=application/json&srsname=EPSG:4326";
//            String queryStr = request.getQueryString();
//            String reqUrl = wfsIp+"/geoserver/wfs?"+queryStr;
            String contentType = "application/json; charset=UTF-8";
            transRequest(reqUrl, contentType, request, response);
        } catch (Exception e) {
//            System.out.println(e);
            response.setStatus(500);
        }
    }

    @SuppressWarnings("deprecation")
    @RequestMapping(value = "/queryId")
    @ResponseBody
    public void queryId(HttpServletRequest request, HttpServletResponse response) {

        try {
            String typeName = request.getParameter("typeName");
            String Id = request.getParameter("Id");
            String reqUrl = wfsIp + "/geoserver/wfs?service=WFS&request=GetFeature&version=1.1.0&typename=" + typeName + "&outputFormat=json&cql_filter=GridId=" + Id;
            String contentType = "application/json; charset=UTF-8";
            transRequest(reqUrl, contentType, request, response);
        } catch (Exception e) {
            response.setStatus(500);
        }

    }

    @SuppressWarnings("deprecation")
    @RequestMapping(value = "/getFeature")
    @ResponseBody
    public void getFeature(HttpServletRequest request, HttpServletResponse response) {
        try {
            String queryStr = request.getQueryString();
//            System.out.println(queryStr);
            if (queryStr != null) {
                String reqUrl = wfsIp + "/geoserver/wfs?" + queryStr;
                String contentType = "application/json; charset=UTF-8";
                transRequest(reqUrl, contentType, request, response);
            } else {
                response.setStatus(404);
            }
        } catch (Exception e) {
            response.setStatus(500);
        }
    }

    @SuppressWarnings("deprecation")
    @RequestMapping(value = "/getFeatureFilter")
    @ResponseBody
    public void getFeatureFilter(HttpServletRequest request, HttpServletResponse response) {

        try {
            String queryStr = request.getQueryString();
            System.out.println(queryStr);
            if (queryStr != null) {
                String reqUrl = wfsIp + "/geoserver/wfs?" + queryStr;
                //String reqUrl = "http://127.0.0.1:8088/ytserver/wfs?service=WFS&request=GetFeature&version=1.1.0&typename=HXMAP_ADMIN:WG_LAYERS&outputFormat=json&cql_filter=GridId=";
                String contentType = "application/json; charset=UTF-8";
                transRequest(reqUrl, contentType, request, response);
            } else {
                response.setStatus(404);
            }
        } catch (Exception e) {
//            System.out.println(e);
            response.setStatus(500);
        }
    }

    @SuppressWarnings("deprecation")
    @RequestMapping(value = "/editFeature")
    @ResponseBody
    public void editFeature(HttpServletRequest request, HttpServletResponse response) {

        try {
            String reqUrl = wfsIp + "/geoserver/wfs";
            String contentType = "application/xml; charset=UTF-8";
            transRequest(reqUrl, contentType, request, response);
        } catch (Exception e) {
//            System.out.println(e);
            response.setStatus(500);
        }
    }


    public void transRequest(String reqUrl, String contentType, HttpServletRequest request, HttpServletResponse response) {
        try {
            URL url = new URL(reqUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            //con.setRequestProperty("content-type", "application/json; charset=UTF-8");
            con.setRequestProperty("content-type", contentType);
            con.setDoOutput(true);
            con.setRequestMethod(request.getMethod());
            //con.setCharacterEncoding("UTF-8");
            int clength = request.getContentLength();
            if (clength > 0) {
                con.setDoInput(true);
                byte[] idata = new byte[clength];
                BufferedReader br = new BufferedReader(new InputStreamReader((ServletInputStream) request.getInputStream()));
                String line1 = null;
                StringBuilder sb = new StringBuilder();
                while ((line1 = br.readLine()) != null) {
                    sb.append(line1);
                }
                byte[] paradata = sb.toString().getBytes();
                con.getOutputStream().write(paradata);
            }
            response.setContentType(con.getContentType());

            BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
            String line;
            StringBuilder sb1 = new StringBuilder();
            PrintWriter out = response.getWriter();
            while ((line = rd.readLine()) != null) {
                //out.println(line);
                sb1.append(line);
            }
            rd.close();
            out.println(sb1);
            out.flush();
            out.close();
        } catch (Exception e) {
            System.out.println(e);
            response.setStatus(500);
        }
    }

//    public static String getXmlString(Document doc){
//        TransformerFactory tf = TransformerFactory.newInstance();
//        try {
//            Transformer t = tf.newTransformer();
//            t.setOutputProperty(OutputKeys.ENCODING,"UTF-8");//解决中文问题，试过用GBK不行
//            //t.setOutputProperty(OutputKeys.METHOD, "html");
//            //t.setOutputProperty(OutputKeys.VERSION, "4.0");
//            //t.setOutputProperty(OutputKeys.INDENT, "no");
//            ByteArrayOutputStream bos = new ByteArrayOutputStream();
//            t.transform(new DOMSource(doc), new StreamResult(bos));
//            return bos.toString();
//        } catch (TransformerConfigurationException e) {
//            e.printStackTrace();
//        } catch (TransformerException e) {
//            e.printStackTrace();
//        }
//        return "";
//    }


}



<%@ page import = "com.mongodb.*" %>
<%@ page import = "com.search.searchweb.MongoDB" %>

<%@ page import = "java.util.*" %>
<%@ page import = "java.util.Map.Entry" %>
<%@ page import = "java.net.UnknownHostException" %>


<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib
    prefix="c"
    uri="http://java.sun.com/jsp/jstl/core" 
%>
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<link rel="stylesheet" type="text/css" href="style.css">
<title>Googly</title>
</head>
<body>
<form action="Search.jsp" method="GET">
<input class = "btnSearchAnother" type="submit" value="Back to Search" />
</form>

<center><h1 class = "title">Googly</h1></center>
<br>
<% out.println("<h3 id = \"search\">Results for \"" + request.getParameter("search") + "\"</h3>"); %>

	<%  
	
	List<String> result = new ArrayList<String>();
	List<String> reqDocs = new ArrayList<String>();
	List<String> reqUrls = new ArrayList<String>();

	String search =	request.getParameter("search");
	String strpage = request.getParameter("page");
	session.setAttribute("str","1");
	
	int pageValue = Integer.parseInt(strpage);
	session.setAttribute("page",(pageValue)+"");

		
	if(strpage!=null && strpage.matches("0")){
	MongoDB mongoObj = new MongoDB();
	mongoObj.fnInit();
	result = mongoObj.fnSearch(search.toLowerCase());
	
	if(result==null)
		session.setAttribute("noresult","1");
	else
		session.setAttribute("noresult","0");
	
	session.setAttribute("result",result);
	session.setAttribute("page","1");
	session.setAttribute("search",search);
	}
	/*
	for(int i=0; i<20;i++){
		
		reqDocs.add(result.get(i));
		
	}
	
	reqUrls = mongoObj.fnGetUrl(reqDocs);

	
	int count = 0;
	
	Iterator itr = reqUrls.iterator();
	while(itr.hasNext()){
		
		count++;
		
		String key = (String)itr.next();
	      out.print("<p><a target = _blank href = " + key + ">" + key + "</a></p>\n");
	}
	*/
	%>
    
   <c:choose>
     <c:when test = "${noresult.equals(str)}"> 
     <div>
     	<br><br><br>
		<Center><h3 id = "noresult">No result found.</h3> </Center>    
		</div>		
	</c:when> 
   <c:otherwise>  
    <div id="resultsDiv">
	<jsp:include page="/WEB-INF/Result.jsp"/>
	</div>    
	</c:otherwise>  
</c:choose>
    
   	
</html>
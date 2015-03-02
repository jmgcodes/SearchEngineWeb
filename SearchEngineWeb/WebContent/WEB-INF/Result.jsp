<%@ page import = "java.util.*" %>
<%@ page import = "java.util.Map.Entry" %>
<%@ page import = "java.net.UnknownHostException" %>
<%@ page import = "com.mongodb.*" %>
<%@ page import = "com.search.searchweb.MongoDB" %>
<%@ page import = "com.search.searchweb.ResultObject" %>

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

<%

List<String> reqDocs = new ArrayList<String>();
List<String>  result = new ArrayList<String>();
Map<String,ResultObject> reqUrls = new HashMap<String,ResultObject>();

MongoDB mongoObj = new MongoDB();
mongoObj.fnInit();
result = (List<String>)session.getAttribute("result");

String strPage = (String)session.getAttribute("page");
int pageValue = Integer.parseInt(strPage);
int minPage = 1;
int maxPage = (int)Math.ceil((double)result.size()/10);

session.setAttribute("minPage","1");
session.setAttribute("maxPage",maxPage+"");


out.print("<p id = \"pages\">Displaying Page: " + pageValue + " of " + maxPage + "</p><br><br>");
%>
<div id = "space"></div>
<hr/>
<%


for(int i=(pageValue-1)*10; i<(((pageValue)*10 > result.size())?(result.size()):(pageValue*10));i++){
	
	reqDocs.add(result.get(i));
	
}

reqUrls = mongoObj.fnGetUrl(reqDocs,session.getAttribute("search").toString());


int count = 0;

Iterator itr = reqUrls.keySet().iterator();
while(itr.hasNext()){
	
	count++;
	
	String key = (String)itr.next();
	ResultObject temp = (ResultObject) reqUrls.get(key);
	
	String title = temp.title;
	String preview = temp.preview;
	
	if(!title.equals("")){
		
	%>
	<p class = "resultPar"><a class = "results" target = "_blank" style="text-decoration:none" href = <%= key %>><%= title %></a></p> 
	<p class = "resultsLink"><%= key %></p> 
	<p class = "preview"><%= preview %></p> 
	
	<%
	
	}
	else{
		
	%>
		<p class = "resultPar"><a class = "results" target = "_blank" style="text-decoration:none" href = <%= key %>><%= key %></a></p> 
		<p class = "preview"><%= preview %></p> 
	
	<%
	
	}
      
}

//session.setAttribute("page",(pageValue+1)+"");

%>
<br/>
	<center>
   <!--  <a class = "nextbtn" href=<%= "\"Main.jsp?search=" + session.getAttribute("search") + "\"" %> >Next ></a>  
 -->  
 	
 	<c:choose>
 	<c:when test = "${maxPage.equals(minPage)}"> 
	</c:when> 
     <c:when test = "${page.equals(minPage)}"> 
     		
   		<a href="Main.jsp?search=<%=session.getAttribute("search")%>&page=<%=pageValue+1%>" ><button  class = "nextbtn" type="button">Next</button></a>
	</c:when> 
   <c:when test = "${page.equals(maxPage)}"> 
   
   		<a href="Main.jsp?search=<%=session.getAttribute("search")%>&page=<%=pageValue-1%>" ><button  class = "nextbtn" type="button">Previous</button></a>
	</c:when> 
   <c:otherwise>  
      	<a href="Main.jsp?search=<%=session.getAttribute("search")%>&page=<%=pageValue-1%>" ><button  class = "nextbtn" type="button">Previous</button></a>
   		<a href="Main.jsp?search=<%=session.getAttribute("search")%>&page=<%=pageValue+1%>" ><button  class = "nextbtn" type="button">Next</button></a>
    
	</c:otherwise>  
</c:choose>
 	
    </center>

    </body>
	
</html>
<%--
    Document   : searchNavigationLinks.yag
    Created on : May 07, 2010, 9:36:39 AM
    Author     : "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
--%>
<%@ include file="/common/taglibs.jsp" %>
<%@ attribute name="totalRecords" required="true" type="java.lang.Long" %>
<%@ attribute name="startIndex" required="true" type="java.lang.Long" %>
<%@ attribute name="pageSize" required="true" type="java.lang.Long" %>
<%@ attribute name="maxPageLinks" required="false" type="java.lang.Integer" %>
<span id="navLinks">
    <c:if test="${empty maxPageLinks}"><c:set var="maxPageLinks" value="10"/></c:if>
    <fmt:formatNumber var="maxIndex" value="${(totalRecords / pageSize) + 1}" pattern="0" />
    <fmt:formatNumber var="pageNumber" value="${(startIndex / pageSize) + 1}" pattern="0" />
    <c:set var="coreParams">?q=${param.q}<c:if test="${not empty paramValues.fq}">&fq=${fn:join(paramValues.fq, "&fq=")}</c:if>&sort=${param.sort}&dir=${param.dir}&pageSize=${pageSize}</c:set>
    <!-- coreParams = ${coreParams} || maxIndex = ${maxIndex} || startIndex = ${startIndex} || pageNumber = ${pageNumber} -->
    <c:set var="startPageLink">
        <c:choose>
            <c:when test="${pageNumber < 6}">
                1
            </c:when>
            <c:otherwise>
                ${pageNumber - 4}
            </c:otherwise>
        </c:choose>
    </c:set>
    <c:set var="endPageLink">
        <c:choose>
            <c:when test="${(pageNumber < (maxIndex - 4))}">
                ${startPageLink + 8}
            </c:when>
            <c:otherwise>
                ${maxIndex}
            </c:otherwise>
        </c:choose>
    </c:set>
    <c:choose>
        <c:when test="${startIndex > 0}">
            <span id="prevPage"><a href="${coreParams}&start=${startIndex - pageSize}#searchResults">&lt; Previous</a></span>
        </c:when>
        <c:otherwise>
            <span id="prevPage">&nbsp;</span>
        </c:otherwise>
    </c:choose>
    <c:forEach var="pageLink" begin="${startPageLink}" end="${endPageLink}" step="1">
        <span id="pageJumpLink">
            <c:choose>
                <c:when test="${pageLink == pageNumber}"><span id="currentPage">${pageLink}</span></c:when>
                <c:otherwise><a href="${coreParams}&start=${(pageLink * pageSize) - pageSize}#searchResults">${pageLink}</a></c:otherwise>
            </c:choose>
        </span>
    </c:forEach>
    <c:choose>
        <c:when test="${!(pageNumber == maxIndex)}">
            <span id="nextPage"><a href="${coreParams}&start=${startIndex + pageSize}#searchResults">Next &gt;</a></span>
        </c:when>
        <c:otherwise>
            <span id="nextPage">&nbsp;</span>
        </c:otherwise>
    </c:choose>
</span>
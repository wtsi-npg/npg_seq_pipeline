<?xml version="1.0"?> 
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
version="1.0"> 
<xsl:output method="html" indent="yes"/>

<!--Author: MZ!-->

<xsl:template match="/"> 

<html>
<body>

   <h1>Summary Information For Experiment <xsl:value-of select="Summary/ChipSummary/RunFolder"/> </h1> 

<xsl:if test="string(Summary/ChipSummary)">
   <h2>Chip Summary</h2>

   <table border="1" cellpadding="5">
      <xsl:for-each select="Summary/ChipSummary"> 

         <tr><td >Machine</td>
         <td> <xsl:value-of select="Machine"/> </td></tr>

         <tr><td >Run Folder</td>
         <td> <xsl:value-of select="RunFolder"/> </td></tr>

         <tr><td >Chip ID</td>
         <td> <xsl:value-of select="ChipID"/> </td></tr>
      </xsl:for-each> 
   </table> 
</xsl:if>


<xsl:if test="string(Summary/ChipResultsSummary)">
   <h2>Chip Results Summary</h2>

   <table border="1" cellpadding="5">
      <tr><td>Clusters</td>
      <td>Clusters (PF)</td>
      <td>Yield (kbases)</td></tr>

      <xsl:for-each select="Summary/ChipResultsSummary">
         <tr><td><xsl:value-of select="clusterCountRaw"/> </td>

         <td><xsl:value-of select="clusterCountPF"/> </td>
         <td><xsl:value-of select="round(yield div 1000)"/></td>
         </tr>
      </xsl:for-each>
   </table>
</xsl:if>

<xsl:if test="string(Summary/Samples)">
  <h2><br/>Samples summary</h2>

  <table border="1" cellpadding="5">
    <tr>
      <th>Lane</th>
      <th>Barcode</th>
      <th>Sample</th>
      <th>Species</th>
    </tr>

    <xsl:for-each select="Summary/Samples/Lane">
      <tr>
        <td><xsl:value-of select="laneNumber"/> </td>
        <td><xsl:value-of select="barcode"/> </td>
        <td><xsl:value-of select="sampleId"/> </td>
        <td><xsl:value-of select="species"/></td>
      </tr>

    </xsl:for-each>
  </table>
</xsl:if>

<h2>Lane Parameter Summary</h2>

<table border="1" cellpadding="5">
   <tr><td>Lane </td>
   <td>Sample ID</td>
   <td>Sample Target</td>

   <td>Sample Type</td>
   <td>Length</td>
   <td>Filter</td>
   <td>Chast. Thresh.</td>
   <td>Num Tiles</td>
   <td>Tiles</td></tr>

   <xsl:for-each select="Summary/LaneParameterSummary/Lane">

      <tr>
      <td><xsl:value-of select="laneNumber"/> </td>
      <td><xsl:value-of select="sample"/> </td>
      <td><xsl:value-of select="template"/></td>
      <td><xsl:value-of select="type"/> </td>

      <xsl:choose>
         <xsl:when test="lengthsList[.!='unknown']">
            <td><xsl:value-of select="lengthsList"/> </td>
         </xsl:when>
         <xsl:otherwise>
            <td><xsl:value-of select="originalReadLength"/> </td>
         </xsl:otherwise>

      </xsl:choose>

      <td><xsl:value-of select="purity"/> </td>
      <td><xsl:value-of select="chastityThreshold"/> </td>
      <td><xsl:value-of select="tileCountRaw"/> </td>

      <td>
      <xsl:element name="a">

      <xsl:attribute 
	name="href">#Lane<xsl:value-of select="laneNumber"/></xsl:attribute>
        Lane <xsl:value-of select="laneNumber"/>
      </xsl:element>
      </td></tr>

   </xsl:for-each>
</table>


<xsl:for-each select="Summary/LaneResultsSummary/Read">

   <xsl:variable name="numReads" select="count(../Read)"/>

   <h2>Lane Results Summary<xsl:if test="count(../Read)>1"> : Read <xsl:value-of select="readNumber"/></xsl:if></h2>

   <table border="1"  cellpadding="5">
      <tr><td colspan="2">Lane Info</td>
      <td colspan="8">Tile Mean +/- SD for Lane</td></tr>

      <tr><td>Lane </td>
      <td>Lane Yield (kbases) </td>

      <td>Clusters (raw)</td>
      <td>Clusters (PF) </td>
      <td>1st Cycle Int (PF) </td>
      <td>% intensity after 20 cycles (PF) </td>

      <td>% PF Clusters </td>
      <td>% Align (PF) </td>
      <td>Alignment Score (PF) </td>
      <td>% Error Rate (PF) </td></tr>

      <xsl:variable name="clusterCountRawMean" select="sum(Lane/clusterCountRaw/mean)"/>
      <xsl:variable name="clusterCountPFMean" select="sum(Lane/clusterCountPF/mean)"/>

      <xsl:variable name="oneSigMean" select="sum(Lane/oneSig/mean)"/>
      <xsl:variable name="signal20AsPctOf1Mean" select="sum(Lane/signal20AsPctOf1/mean)"/>
      <xsl:variable name="percentClustersPFMean" select="sum(Lane/percentClustersPF/mean)"/>
      <xsl:variable name="percentUniquelyAlignedPFMean" select="sum(Lane/percentUniquelyAlignedPF/mean)"/>
      <xsl:variable name="averageAlignScorePFMean" select="sum(Lane/averageAlignScorePF/mean)"/>
      <xsl:variable name="errorPFMean" select="sum(Lane/errorPF/mean)"/>
      <xsl:variable name="numLanes" select="count(Lane/clusterCountRaw/mean)"/>
      <xsl:variable name="numErrorLanes" select="count(Lane/errorPF/mean)"/>
      
      <xsl:for-each select="Lane">

         <xsl:if test="string(laneYield)">

         <tr>
         <td><xsl:value-of select="laneNumber"/> </td>

         <td><xsl:value-of select="laneYield"/> </td>

         <td>
             <xsl:value-of select="clusterCountRaw/mean"/> +/- <xsl:value-of select="clusterCountRaw/stdev"/> 
         </td>

         <td>
             <xsl:value-of select="clusterCountPF/mean"/> +/- <xsl:value-of select="clusterCountPF/stdev"/> 
         </td>
         <td>
             <xsl:value-of select="oneSig/mean"/> +/- <xsl:value-of select="oneSig/stdev"/>
         </td>
         <td>

            <xsl:value-of select="signal20AsPctOf1/mean"/> +/- <xsl:value-of select="signal20AsPctOf1/stdev"/>
         </td>
         <td>
            <xsl:value-of select="percentClustersPF/mean"/> +/- <xsl:value-of select="percentClustersPF/stdev"/>
         </td>

         <xsl:choose>

            <xsl:when test="string(percentUniquelyAlignedPF/mean)">
               <td><xsl:value-of select="percentUniquelyAlignedPF/mean"/> +/- <xsl:value-of select="percentUniquelyAlignedPF/stdev"/> </td>
            </xsl:when>
            <xsl:otherwise>
               <td>0</td>
            </xsl:otherwise>
         </xsl:choose>

         <xsl:choose>
            <xsl:when test="string(averageAlignScorePF/mean)">
               <td><xsl:value-of select="averageAlignScorePF/mean"/> +/- <xsl:value-of select="averageAlignScorePF/stdev"/> </td>
            </xsl:when>
            <xsl:otherwise>
               <td>0</td>

            </xsl:otherwise>
         </xsl:choose>

         <xsl:choose>
            <xsl:when test="string(errorPF/mean)">
               <td><xsl:value-of select="errorPF/mean"/> +/- <xsl:value-of select="errorPF/stdev"/> </td>
            </xsl:when>
            <xsl:otherwise>

               <td>0</td>
            </xsl:otherwise>
         </xsl:choose>
   
         </tr>

         </xsl:if>
 
      </xsl:for-each>


      <tr><td  colspan="13">Tile mean across chip</td></tr>

      <tr><td  colspan="2">Average</td>

      <td><xsl:value-of select="round($clusterCountRawMean div $numLanes)"/> </td>
      <td><xsl:value-of select="round($clusterCountPFMean div $numLanes)"/> </td>
      <td><xsl:value-of select="round($oneSigMean div $numLanes)"/> </td>
      <td><xsl:value-of select="round($signal20AsPctOf1Mean div $numLanes * 100) div 100"/> </td>

      <td><xsl:value-of select="round($percentClustersPFMean div $numLanes * 100) div 100"/> </td>
      <td><xsl:value-of select="round($percentUniquelyAlignedPFMean div $numErrorLanes * 100) div 100"/> </td>
      <td><xsl:value-of select="round($averageAlignScorePFMean div $numErrorLanes * 100) div 100"/> </td>
      <td><xsl:value-of select="round($errorPFMean div $numErrorLanes * 100) div 100"/> </td>
      </tr>
   </table>
</xsl:for-each>


<xsl:for-each select="Summary/ExpandedLaneSummary/Read">

   <xsl:variable name="numReads" select="count(../Read)"/>

   <h2>Expanded Lane Summary<xsl:if test="count(../Read)>1"> : Read <xsl:value-of select="readNumber"/></xsl:if></h2>

   <table border="1" cellpadding="5">
   <tr>

    <td colspan="2">Lane Info</td>
    <td colspan="2">Phasing Info</td>
    <td colspan="2">Raw Data (tile mean)</td>
    <td colspan="7">Filtered Data (tile mean)</td>
   </tr>
   <tr>
     <td>Lane </td>

     <td> Clusters (tile mean) (raw) </td>
     <td>% Phasing </td>
     <td>% Prephasing </td>
     <td>% Error Rate (raw) </td> 	 
     <td> Equiv Perfect Clusters (raw) </td> 
     <td>% retained </td>

     <td>Cycle 2-4 Av Int (PF) </td>
     <td>Cycle 2-10 Av % Loss (PF) </td>
     <td>Cycle 10-20 Av % Loss (PF) </td>
     <td>% Align (PF) </td>	
     <td> % Error Rate (PF) </td> 	 
     <td> Equiv Perfect Clusters (PF) </td></tr> 

   <xsl:for-each select="Lane">

      <xsl:if test="string(clusterCountRaw)">

      <tr><td><xsl:value-of select="laneNumber"/> </td>
      <td><xsl:value-of select="round(clusterCountRaw/mean)"/> </td>
      <td><xsl:value-of select="phasingApplied"/> </td>
      <td><xsl:value-of select="prephasingApplied"/> </td>

      <xsl:choose>
         <xsl:when test="string(errorRaw/mean)">
            <td><xsl:value-of select="errorRaw/mean"/> </td>
         </xsl:when>
         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>

      <xsl:choose>
         <xsl:when test="string(infoContentRaw/mean)">
            <td><xsl:value-of select="infoContentRaw/mean"/> </td>
         </xsl:when>
         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>

      </xsl:choose>

      <td><xsl:value-of select="percentClustersPF/mean"/> </td>

      <td><xsl:value-of select="signalAverage2to4/mean"/> +/- <xsl:value-of select="signalAverage2to4/stdev"/> </td>

      <td><xsl:value-of select="signalLoss2to10/mean"/> +/- <xsl:value-of select="signalLoss2to10/stdev"/> </td>

      <td><xsl:value-of select="signalLoss10to20/mean"/> +/- <xsl:value-of select="signalLoss10to20/stdev"/> </td>

      <xsl:choose>
         <xsl:when test="string(percentUniquelyAlignedPF/mean)">
            <td><xsl:value-of select="percentUniquelyAlignedPF/mean"/> </td>
         </xsl:when>
         <xsl:otherwise>

            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>

      <xsl:choose>
         <xsl:when test="string(errorPF/mean)">
            <td><xsl:value-of select="errorPF/mean"/> </td>
         </xsl:when>

         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>

      <xsl:choose>
         <xsl:when test="string(infoContentPF/mean)">
            <td><xsl:value-of select="infoContentPF/mean"/></td>
         </xsl:when>

         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>
      </tr>
    
      </xsl:if>

   </xsl:for-each>
   </table>

</xsl:for-each>



<xsl:for-each select="Summary/TileResultsByLane/Lane">

<xsl:variable name="numReads" select="count(Read)"/>

<xsl:for-each select="Read">

   <xsl:element name="a">
   <xsl:attribute name="name">Lane<xsl:value-of select="../laneNumber"/>
   </xsl:attribute>

   <h2>Lane <xsl:value-of select="../laneNumber"/><xsl:if test="count(../Read)>1"> : Read <xsl:value-of select="readNumber"/></xsl:if></h2>

</xsl:element>


   <table border="1" cellpadding="5">

   <tr><td colspan="1">Lane </td>
   <td colspan="1">Tile</td>

   <td colspan="1">Clusters (raw)</td>
   <td colspan="1">Av 1st Cycle Int (PF)</td>
   <td colspan="1">Av % intensity after 20 cycles (PF)</td>
   <td colspan="1">% PF Clusters </td>
   <td colspan="1">% Align (PF) </td>
   <td colspan="1">Av Alignment Score (PF)</td>

   <td colspan="1">% Error Rate (PF) </td></tr>

   <xsl:for-each select="Tile">
      <tr>
      <td><xsl:value-of select="../../laneNumber"/> </td>
      <td><xsl:value-of select="tileNumber"/> </td>
      <td><xsl:value-of select="clusterCountRaw"/> </td>

      <td><xsl:value-of select="oneSig"/> </td>
      <td><xsl:value-of select="signal20AsPctOf1"/> </td>
      <td><xsl:value-of select="percentClustersPF"/> </td>

      <xsl:choose>
         <xsl:when test="string(percentUniquelyAlignedPF)">
            <td><xsl:value-of select="percentUniquelyAlignedPF"/></td>
         </xsl:when>

         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>

      <xsl:choose>
         <xsl:when test="string(averageAlignScorePF)">
            <td><xsl:value-of select="averageAlignScorePF"/></td>
         </xsl:when>

         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>

      <xsl:choose>
         <xsl:when test="string(errorPF)">
            <td><xsl:value-of select="errorPF"/></td>
         </xsl:when>

         <xsl:otherwise>
            <td>0</td>
         </xsl:otherwise>
      </xsl:choose>
      </tr>
   </xsl:for-each>
   </table>
   </xsl:for-each>

</xsl:for-each>


   <li><h3>IVC Plots</h3></li>
   <xsl:element name="a">
   <xsl:attribute name="href">../IVC.htm</xsl:attribute> click here 
   </xsl:element>


   <h3><li>All Intensity Plots</li></h3>

   <xsl:element name="a">
   <xsl:attribute name="href">../All.htm</xsl:attribute> click here
   </xsl:element>

   <h3><li>Error Graphs</li></h3>
   <xsl:element name="a">
   <xsl:attribute name="href">Error.htm</xsl:attribute> click here
   </xsl:element>

   <h3><li>Error Curves</li></h3>
   <xsl:element name="a">
   <xsl:attribute name="href">Perfect.htm</xsl:attribute> click here
   </xsl:element>

<!--Coverage plots, monotemplates and pair summaries go here!-->

<xsl:for-each select="Summary/TileErrorsByLane/Lane">

   <xsl:if test="Monotemplate[.!='']">

      <h3>Monotemplate Summary</h3>

      <table border="1" cellpadding="5">
      <tr><th colspan="1">Template</th>
      <th colspan="1">Count</th>
      <th colspan="1">Percent</th>
      <th colspan="1">True 1st Cycle Intensity</th>

      <th colspan="1">Av Error Rate</th>
      <th colspan="1">% Perfect</th></tr>

      <xsl:for-each select="Monotemplate/TemplateList">
         <tr>
         <td><xsl:value-of select="Template"/> </td>
         <td><xsl:value-of select="Count"/> </td>

         <td><xsl:value-of select="Percent"/> </td>
         <td><xsl:value-of select="TrueFirstCycleIntensity"/> </td>
         <td><xsl:value-of select="AvErrorRate"/> </td>
         <td><xsl:value-of select="PercentsPerfect"/> </td>
         </tr>
      </xsl:for-each>
      </table>

      <h3>Monotemplate IVC plot:</h3>
      <xsl:element name="a">
      <xsl:attribute name="href">Monotemplate.htm</xsl:attribute> Monotemplate.htm
      </xsl:element>

   </xsl:if>

</xsl:for-each>

<xsl:if test="/Summary/PairSummary[.!='']">

<center>
<h1>Pair Summary Information</h1>
</center>

<xsl:for-each select="/Summary/PairSummary">


<xsl:if test="fileName[.!='']">

<xsl:variable name="fileName" select="fileName"/>
<xsl:variable name="pairinfo"
select="document($fileName)"/>

<xsl:variable name="fmr2r1" select="$pairinfo//ReadPairProperties/Orientation/Fm"/>

<xsl:variable name="fpr1r2" select="$pairinfo//ReadPairProperties/Orientation/Fp"/>
<xsl:variable name="rmr2r1" select="$pairinfo//ReadPairProperties/Orientation/Rm"/>
<xsl:variable name="rpr1r2" select="$pairinfo//ReadPairProperties/Orientation/Rp"/>
<xsl:variable name="nominal_orient" select="$pairinfo//ReadPairProperties/Orientation/Nominal"/>

<xsl:variable name="divisor" select="$fmr2r1 + $fpr1r2 + $rmr2r1 + $rpr1r2"/>

<!-- Achtung: if divisor is 0 then prints infinity !-->
<xsl:variable name="fmr2r1_err" select="$fmr2r1 div $divisor"/>
<xsl:variable name="fpr1r2_err" select="$fpr1r2 div $divisor"/>
<xsl:variable name="rmr2r1_err" select="$rmr2r1 div $divisor"/>
<xsl:variable name="rpr1r2_err" select="$rpr1r2 div $divisor"/>

 <h2>Lane <xsl:value-of select="laneNumber"/>: </h2>

<h3>Relative Orientation Statistics</h3>
<table border="1" cellpadding="5">
   <tr>
   <th>F-: &gt; R2 R1 &gt;</th>
   <th>F+: &gt; R1 R2 &gt;</th>
   <th>R-: &lt; R2 R1 &gt;</th>

   <th>R+: &gt; R1 R2 &lt;</th>
   <th>Total</th>
   </tr>

 <tr><td>
        <xsl:if test="$nominal_orient[.='Fm']">
	<b><xsl:value-of select="$fmr2r1"/> (<xsl:value-of select="round($fmr2r1_err*1000) div 10"/>%)</b>

	</xsl:if>
        <xsl:if test="$nominal_orient[.!='Fm']">
	<xsl:value-of select="$fmr2r1"/> (<xsl:value-of select="round($fmr2r1_err*1000) div 10"/>%)
	</xsl:if>
     </td>
     <td>
        <xsl:if test="$nominal_orient[.='Fp']">
	<b><xsl:value-of select="$fpr1r2"/> (<xsl:value-of select="round($fpr1r2_err*1000) div 10"/>%)</b>

	</xsl:if>
        <xsl:if test="$nominal_orient[.!='Fp']">
	<xsl:value-of select="$fpr1r2"/> (<xsl:value-of select="round($fpr1r2_err*1000) div 10"/>%)
	</xsl:if>
     </td>
     <td>
        <xsl:if test="$nominal_orient[.='Rm']">
	<b><xsl:value-of select="$rmr2r1"/> (<xsl:value-of select="round($rmr2r1_err*1000) div 10"/>%)</b>

	</xsl:if>
        <xsl:if test="$nominal_orient[.!='Rm']">
	<xsl:value-of select="$rmr2r1"/> (<xsl:value-of select="round($rmr2r1_err*1000) div 10"/>%)
	</xsl:if>
     </td>
     <td>
        <xsl:if test="$nominal_orient[.='Rp']">
	<b><xsl:value-of select="$rpr1r2"/> (<xsl:value-of select="round($rpr1r2_err*1000) div 10"/>%)</b>

	</xsl:if>
        <xsl:if test="$nominal_orient[.!='Rp']">
	<xsl:value-of select="$rpr1r2"/> (<xsl:value-of select="round($rpr1r2_err*1000) div 10"/>%)
	</xsl:if>
     </td>
     <td>
        <xsl:value-of select="$fmr2r1 + $fpr1r2 + $rmr2r1 + $rpr1r2"/>
     </td>

 </tr>
</table>

<h3>Insert Size Statistics</h3>
<h4>(for relative orientation 
<xsl:choose>
   <xsl:when test="$nominal_orient[.='Fm']">F-</xsl:when>
   <xsl:when test="$nominal_orient[.='Fp']">F+</xsl:when>
   <xsl:when test="$nominal_orient[.='Rm']">R-</xsl:when>

   <xsl:when test="$nominal_orient[.='Rp']">R+</xsl:when>
   <xsl:otherwise>unknown</xsl:otherwise>
</xsl:choose>
)</h4>
<table border="1" cellpadding="5">
   <tr>
   <th>Median</th>
   <th>Below-median SD</th>

   <th>Above-median SD</th>
   <th>Low thresh.</th>
   <th>High thresh.</th>
   </tr>

<tr>
   <xsl:choose>
       <xsl:when test="string($pairinfo/ReadPairProperties/InsertSize/Median)">

          <td><xsl:value-of select="$pairinfo/ReadPairProperties/InsertSize/Median"/> </td>
       </xsl:when>
       <xsl:otherwise><td></td></xsl:otherwise>
    </xsl:choose>

   <xsl:choose>
       <xsl:when test="string($pairinfo/ReadPairProperties/InsertSize/LowSD)">
          <td><xsl:value-of select="$pairinfo/ReadPairProperties/InsertSize/LowSD"/> </td>

       </xsl:when>
       <xsl:otherwise><td></td></xsl:otherwise>
    </xsl:choose>

   <xsl:choose>
       <xsl:when test="string($pairinfo/ReadPairProperties/InsertSize/HighSD)">
          <td><xsl:value-of select="$pairinfo/ReadPairProperties/InsertSize/HighSD"/> </td>
       </xsl:when>
       <xsl:otherwise><td></td></xsl:otherwise>

    </xsl:choose>

    <xsl:choose>
       <xsl:when test="string($pairinfo/ReadPairProperties/InsertSize/Min)">
          <td><xsl:value-of select="$pairinfo/ReadPairProperties/InsertSize/Min"/> </td>
       </xsl:when>
       <xsl:otherwise><td></td></xsl:otherwise>
    </xsl:choose>

    <xsl:choose>
       <xsl:when test="string($pairinfo/ReadPairProperties/InsertSize/Max)">
          <td><xsl:value-of select="$pairinfo/ReadPairProperties/InsertSize/Max"/> </td>
       </xsl:when>
       <xsl:otherwise><td></td></xsl:otherwise>
    </xsl:choose>

 </tr>
</table>

<h3>Insert Statistics </h3><h4>(% of individually uniquely alignable pairs)</h4>
<table border="1" cellpadding="5">
   <tr>
   <th>Too small</th>
   <th>Too large</th>
   <th>Orientation and size OK</th>
   </tr>

<xsl:variable name="ts" select="$pairinfo/ReadPairProperties/Orientation/NominalOrientationButSmallInsert"/>
<xsl:variable name="tl" select="$pairinfo/ReadPairProperties/Orientation/NominalOrientationButLargeInsert"/>
<xsl:variable name="osOK" select="$pairinfo/ReadPairProperties/Reads/Read1SingleAlignmentFound/
                                 Read2SingleAlignmentFound/UniquePairedAlignment"/>

<!-- Warning: if divisor is 0 then prints infinity !-->
<xsl:variable name="ts_err" select="round($ts div $divisor * 1000) div 10"/>
<xsl:variable name="tl_err" select="round($tl div $divisor * 1000) div 10"/>
<xsl:variable name="osOK_err" select="round($osOK div $divisor * 1000) div 10"/>

 <tr><td>
        <xsl:value-of select="$ts"/>
        (<xsl:value-of select="$ts_err"/>%)
     </td>

     <td>
        <xsl:value-of select="$tl"/>
        (<xsl:value-of select="$tl_err"/>%)
     </td>
     <td>
        <b><xsl:value-of select="$osOK"/>
        (<xsl:value-of select="$osOK_err"/>%)</b>
     </td>

 </tr>
</table>

</xsl:if>

</xsl:for-each>

</xsl:if>

<!--!-->

<hr/>
<p><font size="-2">CASAVA-1.6.0a10</font></p>
</body>
</html>

</xsl:template>


</xsl:stylesheet>

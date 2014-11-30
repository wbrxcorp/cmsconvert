import java.io.{File, FileNotFoundException}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.google.common.io.Files
import scalikejdbc.{SQLInterpolation, AutoSession, ConnectionPool}

import scala.util.matching.Regex

/**
 * Created by shimarin on 14/11/22.
 */
object Converter extends SQLInterpolation {
  case class Entry(prefix:String,name:String,
                    userId:Int,title:String,format:String,visible:Boolean,
                    description:Option[String],pageImage:Option[String],content:String,publishedAt:Option[String],
                    labels:List[String],data:String)

  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(new DefaultScalaModule())

  def recursiveListFiles(f: File, r: Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
  }

  def load(htmlFile:String,jsonFile:String,prefix:String,name:String,users:Map[String,Int]):Entry = {
    val json = try {
      objectMapper.readValue(new File(jsonFile), classOf[Map[String, Any]])
    }
    catch {
      case e:FileNotFoundException => Map[String,Any]()
    }
    // 必須項目
    val userId = users(json.get("author").map(_.asInstanceOf[String]).getOrElse("wbrxcorp"))
    val title = json.get("title").map(_.asInstanceOf[String]).getOrElse("NO TITLE")
    val format = "html"
    val visible = true

    // その他項目
    val description = json.get("description").map(_.asInstanceOf[String])
    val pageImage = json.get("page_image").map(_.asInstanceOf[String])
    val content = Files.toString(new File(htmlFile), Charsets.UTF_8)
    val publishedAt = json.get("pubDate").map(_.asInstanceOf[String])
    val labels = json.get("labels").map(_.asInstanceOf[List[String]]).getOrElse(Nil)

    val data = json.filterNot { row =>
      Seq("author","title","description","page_image","pubDate","labels").exists(_ == row._1)
    }
    Entry(prefix,name,userId,title,format,visible,description,pageImage,content,publishedAt,labels,objectMapper.writeValueAsString(data))
  }

  def main(args: Array[String]): Unit = {
    val dbPath = args(0)
    val basePath = args(1).replaceFirst("/+$", "")
    val basePrefix = args(2)

    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:%s".format(dbPath), "sa", "")
    implicit val session = AutoSession

    val users = sql"select id,username from users".map(row=>(row.string(2),row.int(1))).list().apply().toMap

    recursiveListFiles(new File(basePath),"\\.html$".r).map { file =>
      val filePath = file.getPath
      val contentPath = (basePrefix + filePath.substring(basePath.length).replaceFirst("\\.html$", "")).split("/")
      (filePath, filePath.replaceFirst("\\.html$", ".json"),
        contentPath.dropRight(1).mkString("/"), contentPath.last)
    }.filter(_._3 != "templates").foreach { case (htmlFile, jsonFile, prefix, name) =>
      //println(htmlFile, jsonFile, prefix, name)
      val entry = load(htmlFile,jsonFile, prefix, name, users)
      sql"""insert into entries(user_id,prefix,name,title,description,page_image,content,format,data,visible,published_at)
         values(${entry.userId},${entry.prefix},${entry.name},${entry.title},${entry.description},${entry.pageImage},
         ${entry.content},${entry.format},${entry.data},${entry.visible},${entry.publishedAt})""".update().apply()
      val entryId = sql"select id from entries where prefix=${entry.prefix} and name=${entry.name}".map(_.int(1)).single().apply().get
      sql"delete from entries_labels where entry_id=${entryId}".update().apply()
      entry.labels.foreach { label =>
        sql"merge into entries_labels(entry_id,label) values(${entryId},${label})".update().apply()
      }
    }

    println(sql"""
      select * from entries""".map(_.toMap()).list().apply())
  }
}

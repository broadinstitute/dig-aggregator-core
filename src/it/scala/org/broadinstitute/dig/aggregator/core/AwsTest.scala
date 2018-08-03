package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import cats.effect.IO
import com.amazonaws.services.s3.model.S3Object
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import java.nio.charset.Charset

/**
 * @author clint
 * Jul 27, 2018
 */
final class AwsTest extends AwsFunSuite {
  //Config file needs to be in place before this test will work.
  private val opts = new Opts[Config](Array("--config", "src/it/resources/config.json"))
  
  override protected val aws = new AWS[Config](opts)
  
  /**
   * Put() an object, then get() it  
   */
  testWithPseudoDirIO("PutGet") {
    doPutGetTest
  }
  
  /**
   * Make a pseudo-dir, then make the same one again with different metadata
   */
  testWithPseudoDirIO("Mkdir") {
    doMkdirTest
  }
  
  /**
   * Create 1 non-pseudo-dir object and list it
   */
  testWithPseudoDirIO("PutLsNonDir") { 
    doPutLsOneObjectTest(_ + "/foo")
  }
  
  /**
   * Create 1 pseudo-dir object and list it
   */
  testWithPseudoDirIO("PutLsDir") { 
    doPutLsOneObjectTest(_ + "/foo/")
  }
  
  /**
   * Create 1 object inside a pseudo-dir and list it
   */
  testWithPseudoDirIO("PutLs1") { 
    doPutLsTest(1)
  }
  
  /**
   * Create 10 objects and list them
   */
  testWithPseudoDirIO("PutLs10") { 
    doPutLsTest(10)
  }
  
  /**
   * Create 2500 objects and list them
   */
  testWithPseudoDirIO("PutLs2500") { 
    doPutLsTest(2500)
  }
  
  /**
   * Create 10 objects and delete them
   */
  testWithPseudoDirIO("Rm10") { 
    doRmTest(10) 
  }
  
  /**
   * Create 2500 objects and delete them
   */
  testWithPseudoDirIO("Rm2500") {
    doRmTest(2500)
  }

  //Create one object and list it
  private def doPutLsOneObjectTest(makeKey: String => String): String => IO[Unit] = { pseudoDirKey =>
    val key = makeKey(pseudoDirKey)
    
    for {
      shouldntExist <- aws.ls(key)
      _ <- aws.put(key, "abc")
      shouldExist <- aws.ls(key)
    } yield {
      assert(shouldntExist == Nil)
      
      assert(shouldExist == Seq(key))
    }
  }
  
  //Create n objects, then list them
  private def doPutLsTest(n: Int): String => IO[Unit] = { pseudoDirKey =>
    
    import cats.implicits._
    
    def toKey(i: Int) = s"${pseudoDirKey}/${i}"
    
    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"
    
    for {
      keysBeforePut <- aws.ls(pseudoDirKeyWithSlash)
      _ <- (1 to n).toList.map(i => aws.put(toKey(i), i.toString)).sequence
      keys <- aws.ls(pseudoDirKeyWithSlash)
    } yield {
      assert(keysBeforePut == Nil)
      
      val expected = (1 to n).map(toKey)
      
      //Convert to sets to ignore ordering
      assert(keys.toSet == expected.toSet)
    }
  }
  
  //Create n objects, then delete them
  private def doRmTest(n: Int): String => IO[Unit] = { pseudoDirKey =>

    import cats.implicits._
    
    def toKey(i: Int) = s"${pseudoDirKey}/${i}"
    
    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"
    
    for {
      _ <- (1 to n).toList.map(i => aws.put(toKey(i), i.toString)).sequence
      keysBeforeDeletion <- aws.ls(pseudoDirKeyWithSlash)
      _ <- aws.rm(keysBeforeDeletion)
      keysAfterDeletion <- aws.ls(pseudoDirKeyWithSlash)
    } yield {
      val expectedBeforeDeletion = (1 to n).map(toKey)
      
      //Convert to sets to ignore ordering
      assert(keysBeforeDeletion.toSet == expectedBeforeDeletion.toSet)
      
      //The "containing folder" was never never explicitly created, so it shouldn't exist
      val expectedAfterDeletion = Nil
      
      //Convert to sets to ignore ordering
      assert(keysAfterDeletion == expectedAfterDeletion)
    }
  }
  
  //Put an object, then read it back again
  private lazy val doPutGetTest: String => IO[Unit] = { pseudoDirKey =>
    
    import cats.implicits._
    
    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"
    
    val contents = "asdkljaslkdjalskdjklasdj"
    val key = s"${pseudoDirKey}/some-key"
    
    for {
      beforePut <- aws.ls(pseudoDirKeyWithSlash)
      _ <- aws.put(key, contents)
      contentsFromAws <- aws.get(key).map(asString)
    } yield {
      //sanity check: the thing we're putting shouldn't have been there yet
      assert(beforePut == Nil)
      
      assert(contentsFromAws == contents)
    }
  }

  //Make a pseudo-dir, then make the same one again with different metadata
  private lazy val doMkdirTest: String => IO[Unit] = { pseudoDirKey =>

    import cats.implicits._
    
    val pseudoDirKeyWithSlash = s"$pseudoDirKey/"
    
    val metadataContents0 = "some-metadata0"
    val metadataContents1 = "some-metadata1"
    
    for {
      _ <- aws.mkdir(pseudoDirKey, metadataContents0)
      metadata0 <- aws.get(s"${pseudoDirKey}/metadata").map(asString)
      contents0 <- aws.ls(pseudoDirKeyWithSlash)
      _ <- aws.mkdir(pseudoDirKey, metadataContents1)
      metadata1 <- aws.get(s"${pseudoDirKey}/metadata").map(asString)
      contents1 <- aws.ls(pseudoDirKeyWithSlash)
    } yield {
      assert(metadata0 == metadataContents0)
      
      //Convert to sets to ignore ordering
      assert(contents0.toSet == Set(pseudoDirKeyWithSlash, s"${pseudoDirKey}/metadata"))
      
      assert(metadata1 == metadataContents1)
      
      //Convert to sets to ignore ordering
      assert(contents0.toSet == Set(pseudoDirKeyWithSlash, s"${pseudoDirKey}/metadata"))
    }
  }
  
  private val utf8 = Charset.forName("UTF-8")  
  
  private def asString(s3Object: S3Object): String = {
    try {
      val ois = s3Object.getObjectContent
      
      val chunkSize = 4096
      
      def read(): Array[Byte] = {
        val buffer: Array[Byte] = Array.ofDim(chunkSize)
        
        val numRead = ois.read(buffer)
        
        if(numRead == chunkSize) buffer else buffer.take(numRead)
      }
      
      def notDone(buf: Array[Byte]): Boolean = buf.length > 0
      
      val bytesSoFar: Buffer[Byte] = new ArrayBuffer
      
      val chunks = Iterator.continually(read()).takeWhile(notDone)
      
      chunks.foreach(bytesSoFar ++= _)
      
      new String(bytesSoFar.toArray, utf8)
    } finally {
      s3Object.close()
    }
  }
}

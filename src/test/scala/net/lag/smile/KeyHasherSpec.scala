/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.smile

import org.specs._


object KeyHasherSpec extends Specification {
  // LIST_TO_HASH and known good values from libmemcached test/hash_results.h
  val LIST_TO_HASH = Seq(
    "apple",
    "beat",
    "carrot",
    "daikon",
    "eggplant",
    "flower",
    "green",
    "hide",
    "ick",
    "jack",
    "kick",
    "lime",
    "mushrooms",
    "nectarine",
    "orange",
    "peach",
    "quant",
    "ripen",
    "strawberry",
    "tang",
    "up",
    "volumne",
    "when",
    "yellow",
    "zip")

  val FNV1_32_VALUES = List(
    67176023L, 1190179409L, 2043204404L, 3221866419L,
    2567703427L, 3787535528L, 4147287986L, 3500475733L,
    344481048L, 3865235296L, 2181839183L, 119581266L,
    510234242L, 4248244304L, 1362796839L, 103389328L,
    1449620010L, 182962511L, 3554262370L, 3206747549L,
    1551306158L, 4127558461L, 1889140833L, 2774173721L,
    1180552018L)

  val FNV1A_32_VALUES = List(
    280767167L, 2421315013L, 3072375666L, 855001899L,
    459261019L, 3521085446L, 18738364L, 1625305005L,
    2162232970L, 777243802L, 3323728671L, 132336572L,
    3654473228L, 260679466L, 1169454059L, 2698319462L,
    1062177260L, 235516991L, 2218399068L, 405302637L,
    1128467232L, 3579622413L, 2138539289L, 96429129L,
    2877453236L)

  val FNV1_64_VALUES = List(
    473199127L, 4148981457L, 3971873300L, 3257986707L,
    1722477987L, 2991193800L, 4147007314L, 3633179701L,
    1805162104L, 3503289120L, 3395702895L, 3325073042L,
    2345265314L, 3340346032L, 2722964135L, 1173398992L,
    2815549194L, 2562818319L, 224996066L, 2680194749L,
    3035305390L, 246890365L, 2395624193L, 4145193337L,
    1801941682L)

  val FNV1A_64_VALUES = List(
    1488911807L, 2500855813L, 1510099634L, 1390325195L,
    3647689787L, 3241528582L, 1669328060L, 2604311949L,
    734810122L, 1516407546L, 560948863L, 1767346780L,
    561034892L, 4156330026L, 3716417003L, 3475297030L,
    1518272172L, 227211583L, 3938128828L, 126112909L,
    3043416448L, 3131561933L, 1328739897L, 2455664041L,
    2272238452L)

  val MD5_VALUES = List(
    3195025439L, 2556848621L, 3724893440L, 3332385401L,
    245758794L, 2550894432L, 121710495L, 3053817768L,
    1250994555L, 1862072655L, 2631955953L, 2951528551L,
    1451250070L, 2820856945L, 2060845566L, 3646985608L,
    2138080750L, 217675895L, 2230934345L, 1234361223L,
    3968582726L, 2455685270L, 1293568479L, 199067604L,
    2042482093L)

  val HSIEH_VALUES = List(
    3738850110L, 3636226060L, 3821074029L, 3489929160L, 3485772682L, 80540287L,
    1805464076L, 1895033657L, 409795758L, 979934958L, 3634096985L, 1284445480L,
    2265380744L, 707972988L, 353823508L, 1549198350L, 1327930172L, 9304163L,
    4220749037L, 2493964934L, 2777873870L, 2057831732L, 1510213931L, 2027828987L,
    3395453351L)

  def testLibmemcacheValues(keyHasher: KeyHasher, values: Seq[Long]) {
    "libmemcache values" in {
      LIST_TO_HASH.zip(values).foreach { case (input, expected) =>
        keyHasher.hashKey(input.getBytes) mustEqual expected
      }
    }
  }

  "KeyHasher" should {
    val longKey = "the easter island statue memorial tabernacle choir presents"

    "FNV1-32" in {
      KeyHasher.FNV1_32.hashKey("".getBytes) mustEqual 2166136261L
      KeyHasher.FNV1_32.hashKey("\uffff".getBytes("utf-8")) mustEqual 4055256578L
      KeyHasher.FNV1_32.hashKey("cat".getBytes) mustEqual 983016379L
      KeyHasher.FNV1_32.hashKey(longKey.getBytes) mustEqual 2223726839L
      testLibmemcacheValues(KeyHasher.FNV1_32, FNV1_32_VALUES)
    }

    "FNV1A-32" in {
      KeyHasher.FNV1A_32.hashKey("".getBytes) mustEqual 2166136261L
      KeyHasher.FNV1A_32.hashKey("\uffff".getBytes("utf-8")) mustEqual 21469476L
      KeyHasher.FNV1A_32.hashKey("cat".getBytes) mustEqual 108289031L
      KeyHasher.FNV1A_32.hashKey(longKey.getBytes) mustEqual 1968151335L
      testLibmemcacheValues(KeyHasher.FNV1A_32, FNV1A_32_VALUES)
    }

    "FNV1-64" in {
      KeyHasher.FNV1_64.hashKey("".getBytes) mustEqual 2216829733L
      KeyHasher.FNV1_64.hashKey("\uffff".getBytes("utf-8")) mustEqual 1779777890L
      KeyHasher.FNV1_64.hashKey("cat".getBytes) mustEqual 1806270427L
      KeyHasher.FNV1_64.hashKey(longKey.getBytes) mustEqual 3588698999L
      testLibmemcacheValues(KeyHasher.FNV1_64, FNV1_64_VALUES)
    }

    "FNV1A-64" in {
      KeyHasher.FNV1A_64.hashKey("".getBytes) mustEqual 2216829733L
      KeyHasher.FNV1A_64.hashKey("\uffff".getBytes("utf-8")) mustEqual 2522373860L
      KeyHasher.FNV1A_64.hashKey("cat".getBytes) mustEqual 216310567L
      KeyHasher.FNV1A_64.hashKey(longKey.getBytes) mustEqual 2969891175L
      testLibmemcacheValues(KeyHasher.FNV1A_64, FNV1A_64_VALUES)
    }

    "ketama" in {
      KeyHasher.KETAMA.hashKey("".getBytes) mustEqual 3649838548L
      KeyHasher.KETAMA.hashKey("\uffff".getBytes("utf-8")) mustEqual 844455094L
      KeyHasher.KETAMA.hashKey("cat".getBytes) mustEqual 1156741072L
      KeyHasher.KETAMA.hashKey(longKey.getBytes) mustEqual 3103958980L
      testLibmemcacheValues(KeyHasher.KETAMA, MD5_VALUES)
    }

    "CRC32-ITU" in {
      KeyHasher.CRC32_ITU.hashKey("".getBytes) mustEqual 0L
      KeyHasher.CRC32_ITU.hashKey("\uffff".getBytes("utf-8")) mustEqual 1702646501L
      KeyHasher.CRC32_ITU.hashKey("cat".getBytes) mustEqual 2656977832L
      KeyHasher.CRC32_ITU.hashKey(longKey.getBytes) mustEqual 1410673605L
      // not in libmemcached
    }

    "Hsieh" in {
      KeyHasher.HSIEH.hashKey("".getBytes) mustEqual 0L
      KeyHasher.HSIEH.hashKey("\uffff".getBytes) mustEqual 726644641L
      KeyHasher.HSIEH.hashKey("cat".getBytes) mustEqual 3927732514L
      testLibmemcacheValues(KeyHasher.HSIEH, HSIEH_VALUES)
    }
  }
}

<?php
/*
 * This file is part of the prooph/event-store-doctrine-adapter.
 * (c) 2014-2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 9/4/15 - 9:17 PM
 */
namespace ProophTest\EventStore\Adapter\Doctrine\Schema;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\StringType;
use Doctrine\DBAL\Types\TextType;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\Adapter\Doctrine\Schema\EventStoreSchema;

/**
 * Class EventStoreSchemaTest
 *
 * @package ProophTest\EventStore\Adapter\Doctrine\Schema
 */
final class EventStoreSchemaTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_a_single_stream_schema_correctly()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createSingleStream($schemaTool);

        $table = $schemaTool->getTable('event_stream');

        $this->assertEquals('event_stream', $table->getName());

        $this->assertTrue($table->hasColumn('event_id'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('event_id')->getType());
        $this->assertEquals(36, $table->getColumn('event_id')->getLength());

        $this->assertTrue($table->hasColumn('version'));
        $this->assertInstanceOf(IntegerType::class, $table->getColumn('version')->getType());
        $this->assertNull($table->getColumn('version')->getLength());

        $this->assertTrue($table->hasColumn('event_name'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('event_name')->getType());
        $this->assertEquals(100, $table->getColumn('event_name')->getLength());

        $this->assertTrue($table->hasColumn('payload'));
        $this->assertInstanceOf(TextType::class, $table->getColumn('payload')->getType());
        $this->assertNull($table->getColumn('payload')->getLength());

        $this->assertTrue($table->hasColumn('created_at'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('created_at')->getType());
        $this->assertEquals(50, $table->getColumn('created_at')->getLength());

        $this->assertTrue($table->hasColumn('aggregate_id'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('aggregate_id')->getType());
        $this->assertEquals(36, $table->getColumn('aggregate_id')->getLength());

        $this->assertTrue($table->hasColumn('aggregate_type'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('aggregate_type')->getType());
        $this->assertEquals(100, $table->getColumn('aggregate_type')->getLength());

        $this->assertEquals(['event_id'], $table->getPrimaryKey()->getColumns());

        $this->assertTrue($table->hasIndex('event_stream_m_v_uix'));
        $this->assertEquals(['aggregate_id', 'aggregate_type', 'version'], $table->getIndex('event_stream_m_v_uix')->getColumns());
    }

    /**
     * @test
     */
    public function it_creates_a_single_stream_schema_with_custom_name_correctly()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createSingleStream($schemaTool, 'custom_stream');

        $table = $schemaTool->getTable('custom_stream');

        $this->assertEquals('custom_stream', $table->getName());

        $this->assertTrue($table->hasIndex('custom_stream_m_v_uix'));
        $this->assertEquals(['aggregate_id', 'aggregate_type', 'version'], $table->getIndex('custom_stream_m_v_uix')->getColumns());
    }

    /**
     * @test
     */
    public function it_does_not_include_causation_columns_by_default()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createSingleStream($schemaTool);

        $table = $schemaTool->getTable('event_stream');

        $this->assertFalse($table->hasColumn('causation_id'));
        $this->assertFalse($table->hasColumn('causation_name'));
    }

    /**
     * @test
     */
    public function it_does_add_causation_columns_to_single_stream_when_feature_is_enabled()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createSingleStream($schemaTool, 'event_stream', true);

        $table = $schemaTool->getTable('event_stream');

        $this->assertTrue($table->hasColumn('causation_id'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('causation_id')->getType());
        $this->assertEquals(36, $table->getColumn('causation_id')->getLength());

        $this->assertTrue($table->hasColumn('causation_name'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('causation_name')->getType());
        $this->assertEquals(100, $table->getColumn('causation_name')->getLength());
    }

    /**
     * @test
     */
    public function it_creates_a_aggregate_type_stream_schema_correctly()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createAggregateTypeStream($schemaTool, 'user_stream');

        $table = $schemaTool->getTable('user_stream');

        $this->assertEquals('user_stream', $table->getName());

        $this->assertTrue($table->hasColumn('event_id'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('event_id')->getType());
        $this->assertEquals(36, $table->getColumn('event_id')->getLength());

        $this->assertTrue($table->hasColumn('version'));
        $this->assertInstanceOf(IntegerType::class, $table->getColumn('version')->getType());
        $this->assertNull($table->getColumn('version')->getLength());

        $this->assertTrue($table->hasColumn('event_name'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('event_name')->getType());
        $this->assertEquals(100, $table->getColumn('event_name')->getLength());

        $this->assertTrue($table->hasColumn('payload'));
        $this->assertInstanceOf(TextType::class, $table->getColumn('payload')->getType());
        $this->assertNull($table->getColumn('payload')->getLength());

        $this->assertTrue($table->hasColumn('created_at'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('created_at')->getType());
        $this->assertEquals(50, $table->getColumn('created_at')->getLength());

        $this->assertTrue($table->hasColumn('aggregate_id'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('aggregate_id')->getType());
        $this->assertEquals(36, $table->getColumn('aggregate_id')->getLength());

        $this->assertTrue($table->hasColumn('aggregate_type'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('aggregate_type')->getType());
        $this->assertEquals(100, $table->getColumn('aggregate_type')->getLength());

        $this->assertFalse($table->hasColumn('causation_id'));

        $this->assertFalse($table->hasColumn('causation_name'));

        $this->assertEquals(['event_id'], $table->getPrimaryKey()->getColumns());

        $this->assertTrue($table->hasIndex('user_stream_m_v_uix'));
        $this->assertEquals(['aggregate_id', 'version'], $table->getIndex('user_stream_m_v_uix')->getColumns());
    }

    /**
     * @test
     */
    public function it_does_add_causation_columns_to_aggregate_type_stream_when_feature_is_enabled()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createAggregateTypeStream($schemaTool, 'user_stream', true);

        $table = $schemaTool->getTable('user_stream');

        $this->assertTrue($table->hasColumn('causation_id'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('causation_id')->getType());
        $this->assertEquals(36, $table->getColumn('causation_id')->getLength());

        $this->assertTrue($table->hasColumn('causation_name'));
        $this->assertInstanceOf(StringType::class, $table->getColumn('causation_name')->getType());
        $this->assertEquals(100, $table->getColumn('causation_name')->getLength());
    }

    /**
     * @test
     */
    public function it_drops_event_stream_by_default()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createSingleStream($schemaTool);

        EventStoreSchema::dropStream($schemaTool);

        $this->assertFalse($schemaTool->hasTable('event_stream'));
    }

    /**
     * @test
     */
    public function it_drops_custom_stream()
    {
        $schemaTool = new Schema();

        EventStoreSchema::createAggregateTypeStream($schemaTool, 'user_stream');

        EventStoreSchema::dropStream($schemaTool, 'user_stream');

        $this->assertFalse($schemaTool->hasTable('user_stream'));
    }
}

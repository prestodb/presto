import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import VeloxConBanner from '@site/src/components/VeloxConBanner';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './index.module.css';
import Head from '@docusaurus/Head';

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();

  return (
    <header className={clsx(styles.heroSection)}>
      <div className={clsx('container', styles.heroContainer)}>
        <img
          className={styles.heroLogo}
          src={useBaseUrl('img/velox-logo.svg')}
          alt="Velox Logo"
        />

        <h1 className={styles.heroTitle}>
            An open-source composable
            <br />
            execution engine for data systems
        </h1>

        <div className={styles.buttonGroup}>
          <Link className={clsx('button', styles.primaryButton)} to="https://facebookincubator.github.io/velox/">
            Get Started
          </Link>
          <Link
            className={clsx('button', styles.secondaryButton)}
            to="https://github.com/facebookincubator/velox"
          >
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}


function WhatIsVeloxSection() {
  return (
    <section className={styles.whatIsVeloxSection}>
      <div className="container">
        <div className={styles.contentRow}>
          <div className={styles.textColumn}>
            <h2>What is Velox?</h2>
            <p>
              Velox is a high-performance, open-source execution engine designed for flexibility and reuse. Distributed as a C++ library, it offers modular, composable components that power a wide range of data processing systems from batch and interactive queries to streaming and AI/ML workloads.
            </p>
          </div>
          <div className={styles.imageColumn}>
            <img
              src={useBaseUrl('img/velox-overview.png')}
              alt="Velox Architecture"
              className={styles.architectureImage}
            />
          </div>
        </div>

        {/* New Ecosystem Section */}
        <div className={styles.ecosystemRow}>
          <h2>The Velox Ecosystem</h2>
          <p className={styles.ecosystemDescription}>
            Velox powers a growing ecosystem of open-source projects, from Spark acceleration with Apache Gluten to next-gen query engines like Presto C++. It’s also the foundation for ongoing work in hardware-accelerated execution.
          </p>
          <div className={styles.ecosystemGrid}>
            <div className={styles.ecosystemCard}>
              <img src={useBaseUrl('img/logo-presto.png')} alt="Presto C++" />
              <h3>Presto C++</h3>
              <p>A next-generation query engine built for performance using Velox components.</p>
              <a href="https://prestodb.io/docs/current/presto-cpp.html" target="_blank" rel="noopener">
                Learn More →
              </a>
            </div>
            <div className={styles.ecosystemCard}>
              <img src={useBaseUrl('img/logo-gluten.png')} alt="Apache Gluten" />
              <h3>Apache Gluten</h3>
              <p>An accelerator for Apache Spark that integrates Velox for native execution.</p>
              <a href="https://gluten.apache.org/" target="_blank" rel="noopener">
                Learn More →
              </a>
            </div>
            <div className={styles.ecosystemCard}>
              <img src={useBaseUrl('img/logo-nvidia.png')} alt="NVIDIA cuDF" />
              <h3>NVIDIA cuDF</h3>
              <p>Hardware-accelerated query execution with Velox + GPU acceleration.</p>
              <a href="https://github.com/facebookincubator/velox/tree/main/velox/experimental/cudf" target="_blank" rel="noopener">
                Learn More →
              </a>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function KeyFeatures() {
  return (
    <section className={styles.keyFeaturesSection}>
      <div className="container">
        <h2>Key Features</h2>
        <div className={styles.featureGrid}>
          <div className={styles.featureCard}>
            <img src={useBaseUrl('img/icon-efficiency.png')} alt="Efficiency Icon" />
            <h3>Efficiency</h3>
            <p>
              Democratizes runtime optimizations previously only implemented in individual engines.
            </p>
          </div>

          <div className={styles.featureCard}>
            <img src={useBaseUrl('img/icon-consistency.png')} alt="Consistency Icon" />
            <h3>Consistency</h3>
            <p>
              By leveraging the same execution library, compute engines can expose the exact same functions, data types, and semantics to their users.
            </p>
          </div>

          <div className={styles.featureCard}>
            <img src={useBaseUrl('img/icon-reusability.png')} alt="Reusability Icon" />
            <h3>Reusability</h3>
            <p>
              Features and runtime optimizations available in Velox are developed and maintained once, reducing engineering duplication and promoting reusability.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}

function TheVeloxCommunity() {
  return (
    <section className={styles.usingSection}>
      <div className="container">
        <h2>The Velox Community</h2>
        <div className={styles.logoScrollerWrapper}>
          <div className={styles.logoScroller}>
            <div className={styles.logoTrack}>
              <img className={styles.metaLogo} src={useBaseUrl('img/logo-meta.png')} alt="Meta" />
              <img src={useBaseUrl('img/logo-ibm.png')} alt="IBM" />
              <img src={useBaseUrl('img/logo-nvidia.png')} alt="Nvidia" />
              <img src={useBaseUrl('img/logo-google.png')} alt="Google" />
              <img className={styles.microsoftLogo} src={useBaseUrl('img/logo-microsoft.png')} alt="Microsoft" />
              <img src={useBaseUrl('img/logo-pinterest.png')} alt="Pinterest" />
              <img src={useBaseUrl('img/logo-voltron.png')} alt="Voltron Data" />
              <img src={useBaseUrl('img/logo-datapelago.png')} alt="DataPelago" />
              <img src={useBaseUrl('img/logo-alibaba.png')} alt="Alibaba Cloud" />
              <img src={useBaseUrl('img/logo-uber.png')} alt="Uber" />
              <img src={useBaseUrl('img/logo-bytedance.png')} alt="Bytedance" />
              <img src={useBaseUrl('img/logo-intel.png')} alt="Intel" />

              {/* Repeat for seamless loop */}
              <img className={styles.metaLogo} src={useBaseUrl('img/logo-meta.png')} alt="Meta" />
              <img src={useBaseUrl('img/logo-ibm.png')} alt="IBM" />
              <img src={useBaseUrl('img/logo-nvidia.png')} alt="Nvidia" />
              <img className={styles.googleLogo} src={useBaseUrl('img/logo-google.png')} alt="Google" />
              <img className={styles.microsoftLogo} src={useBaseUrl('img/logo-microsoft.png')} alt="Microsoft" />
              <img src={useBaseUrl('img/logo-pinterest.png')} alt="Pinterest" />
              <img src={useBaseUrl('img/logo-voltron.png')} alt="Voltron Data" />
              <img src={useBaseUrl('img/logo-datapelago.png')} alt="DataPelago" />
              <img src={useBaseUrl('img/logo-alibaba.png')} alt="Alibaba Cloud" />
              <img src={useBaseUrl('img/logo-uber.png')} alt="Uber" />
              <img src={useBaseUrl('img/logo-bytedance.png')} alt="Bytedance" />
              <img src={useBaseUrl('img/logo-intel.png')} alt="Intel" />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function LatestCommunityNews() {
  const scrollRef = React.useRef(null);
  const [showLeftArrow, setShowLeftArrow] = React.useState(false);

  const scrollLeft = () => {
    if (scrollRef.current) {
      scrollRef.current.scrollBy({ left: -300, behavior: 'smooth' });
    }
  };

  const scrollRight = () => {
    if (scrollRef.current) {
      scrollRef.current.scrollBy({ left: 300, behavior: 'smooth' });
    }
  };

  const handleScroll = () => {
    if (scrollRef.current) {
      setShowLeftArrow(scrollRef.current.scrollLeft > 0);
    }
  };

  React.useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.addEventListener('scroll', handleScroll);
    return () => el.removeEventListener('scroll', handleScroll);
  }, []);

  return (
    <section className={styles.latestNewsSection}>
      <div className="container">
        <h2>Latest Community News</h2>

        {/* Community Stats */}
        <div className={styles.statsInline}>
          <div>
            <img src={useBaseUrl('img/icon-github-star.svg')} alt="GitHub Stars" />
            <span>3.7k GitHub Stars</span>
          </div>
          <div>
            <img src={useBaseUrl('img/icon-contributors.svg')} alt="Contributors" />
            <span>330+ Contributors</span>
          </div>
          <div>
            <img src={useBaseUrl('img/icon-commits.svg')} alt="Commits" />
            <span>5k+ Commits</span>
          </div>
        </div>

        {/* Scrollable Tiles */}
        <div className={styles.tileScrollWrapper}>
          {showLeftArrow && (
            <button className={styles.scrollButtonLeft} onClick={scrollLeft} aria-label="Scroll left">
              ←
            </button>
          )}

          <div className={styles.tileScroll} ref={scrollRef}>
            {/* Each tile is boxed */}
            <a href="https://velox-lib.io/blog/velox-primer-part-3" className={styles.newsTile} target="_blank" rel="noopener">
              <span className={styles.newsTag}>Blog</span>
              <h3>Velox Primer: Part 3</h3>
              <p>Diving deeper into Velox internals and optimization strategies.</p>
              <span className={styles.newsDate}>May 2025</span>
            </a>

            <a href="https://www.youtube.com/playlist?list=PLJvBe8nQAEsE0dT7XVIrD8QE-gmuX3Fe6" className={styles.newsTile} target="_blank" rel="noopener">
              <span className={styles.newsTag}>Event</span>
              <h3>VeloxCon 2025</h3>
              <p>Sessions from VeloxCon are now available on-demand on our YouTube channel.</p>
              <span className={styles.newsDate}>April 2025</span>
            </a>

            <a href="https://velox-lib.io/blog/velox-primer-part-2" className={styles.newsTile} target="_blank" rel="noopener">
              <span className={styles.newsTag}>Blog</span>
              <h3>Velox Primer: Part 2</h3>
              <p>Exploring modular execution and system reuse with Velox.</p>
              <span className={styles.newsDate}>March 2025</span>
            </a>

            <a href="https://velox-lib.io/blog/velox-primer-part-1" className={styles.newsTile} target="_blank" rel="noopener">
              <span className={styles.newsTag}>Blog</span>
              <h3>Velox Primer: Part 1</h3>
              <p>Introduction to Velox and why composable engines matter.</p>
              <span className={styles.newsDate}>February 2025</span>
            </a>

            <a href="https://velox-lib.io/blog/velox-query-tracing" className={styles.newsTile} target="_blank" rel="noopener">
              <span className={styles.newsTag}>Blog</span>
              <h3>Velox Query Tracing</h3>
              <p>More about Velox query tracing including how it works, its framework, and future plans.</p>
              <span className={styles.newsDate}>December 2024</span>
            </a>
          </div>

          <button className={styles.scrollButtonRight} onClick={scrollRight} aria-label="Scroll right">
            →
          </button>
        </div>

        {/* LinkedIn CTA */}
        <div className={styles.linkedinMention}>
          <a
            href="https://www.linkedin.com/company/velox-io"
            target="_blank"
            rel="noopener"
            className={styles.linkedinLink}
          >
            <img
              src={useBaseUrl('img/icon-linkedin.png')}
              alt="LinkedIn"
              className={styles.linkedinIcon}
            />
            <span>Follow us on LinkedIn</span>
          </a>
        </div>
      </div>
    </section>
  );
}


export default function Home() {
  const { siteConfig } = useDocusaurusContext();

return (
  <Layout
    title=""
    description="Composable execution engine for high-performance data systems"
  >
    <Head>
      <title>Velox | Open-Source Composable Execution Engine</title>
      <meta
        name="description"
        content="Composable execution engine for high-performance data systems"
      />
    </Head>

    <HomepageHeader />
    <main>
      <VeloxConBanner />
      <WhatIsVeloxSection />
      <KeyFeatures />
      <TheVeloxCommunity />
      <LatestCommunityNews />
      <HomepageFeatures />
    </main>
  </Layout>
  );
}
